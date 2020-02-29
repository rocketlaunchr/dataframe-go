// Copyright 2019-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package utime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/icza/gox/timex"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

var (
	// ErrContainsNil means that the SeriesTime contains nil values.
	ErrContainsNil = errors.New("contains nil")

	// ErrNoPattern means that no pattern was detected.
	ErrNoPattern = errors.New("no pattern detected")
)

// GuessTimeFreqOptions configures how GuessTimeFreq behaves.
type GuessTimeFreqOptions struct {

	// R is used to limit the range of the Series.
	R *dataframe.Range

	// DontLock can be set to true if the series should not be locked.
	DontLock bool
}

// GuessTimeFreq will attempt to guess the time interval in a SeriesTime.
// It will return a string that is compatible with TimeIntervalGenerator.
// It will also return a bool indicating whether the sequence is backwards.
//
// Due to daylight savings and varying days per month, it is not always possible to
// determine a pattern with 100% confidence.
//
// NOTE: As currently implemented, nil values are not tolerated.
func GuessTimeFreq(ctx context.Context, ts *dataframe.SeriesTime, opts ...GuessTimeFreqOptions) (string, bool, error) {

	if len(opts) == 0 {
		opts = append(opts, GuessTimeFreqOptions{R: &dataframe.Range{}})
	} else if opts[0].R == nil {
		opts[0].R = &dataframe.Range{}
	}

	if !opts[0].DontLock {
		ts.Lock()
		defer ts.Unlock()
	}

	// Check for nil, even if R is set to a sub-range
	if ts.ContainsNil(dataframe.DontLock) {
		return "", false, ErrContainsNil
	}

	nRows := ts.NRows(dataframe.DontLock)

	start, end, err := opts[0].R.Limits(nRows)
	if err != nil {
		return "", false, err
	}

	if (end - start) < 1 {
		return "", false, ErrNoPattern
	}

	// Determine if reverse
	reverse := false

	val1 := *ts.Values[start]
	val2 := *ts.Values[start+1]

	if val1.Equal(val2) {
		return "", false, ErrNoPattern
	} else if val1.After(val2) {
		reverse = true
	}

	var wg sync.WaitGroup

	var (
		// Could be an error or string
		ret1 interface{}
		ret2 interface{}
	)

	// Use time.Duration
	wg.Add(1)
	go func() {
		defer wg.Done()

		var d time.Duration
		if reverse {
			d = val1.Sub(val2)
		} else {
			d = val2.Sub(val1)
		}

		timeFreq := d.String()

		err := ValidateSeriesTime(ctx, ts, timeFreq, ValidateSeriesTimeOptions{DontLock: true})
		if err != nil {
			ret1 = err
		} else {
			ret1 = timeFreq
		}
	}()

	if !((reverse && val1.Sub(val2) < 23*time.Hour) || (val2.Sub(val1) < 23*time.Hour)) {
		// Use github.com/icza/gox/timex
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Find all possible timeFreq
			candidates := map[parsed]struct{}{}
			for i := start; i < end; i++ {

				if err := ctx.Err(); err != nil {
					ret2 = err
					return
				}

				val1 := *ts.Values[i]
				val2 := *ts.Values[i+1]

				var years, months, days, hours, mins, secs int

				if reverse {
					years, months, days, hours, mins, secs = timex.Diff(val1, val2)
				} else {
					years, months, days, hours, mins, secs = timex.Diff(val2, val1)
				}

				if hours != 0 || mins != 0 || secs != 0 { // Not compatible with TimeIntervalGenerator
					ret2 = ErrNoPattern
					return
				}

				candidates[parsed{years, months, 0, days}] = struct{}{}
			}

			// Evaluate each candidate timeFreq
			newCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			var wg sync.WaitGroup
			var lock sync.Mutex
			var foundTimeFreq string

			for k := range candidates {
				timeFreq := k.String()

				wg.Add(1)
				go func() {
					defer wg.Done()

					err := ValidateSeriesTime(newCtx, ts, timeFreq, ValidateSeriesTimeOptions{DontLock: true})
					if err == nil {
						// We found a timeFreq
						cancel()
						lock.Lock()
						foundTimeFreq = timeFreq
						lock.Unlock()
					}
				}()
			}

			wg.Wait()

			if foundTimeFreq != "" {
				ret2 = foundTimeFreq
			} else {
				if err := ctx.Err(); err != nil {
					ret2 = err
				} else {
					ret2 = ErrNoPattern
				}
			}
		}()
	} else {
		// DST has the potential to make a day 23 hours.
		// This scenario is less than 23 hours.
		ret2 = ErrNoPattern
	}

	wg.Wait()

	err1, ok1 := ret1.(error)
	err2, ok2 := ret2.(error)

	// If either was an error, then check if it was due to context cancelation.
	if ok1 && (err1 == context.Canceled || err1 == context.DeadlineExceeded) {
		return "", reverse, err1
	}
	if ok2 && (err2 == context.Canceled || err2 == context.DeadlineExceeded) {
		return "", reverse, err2
	}

	if ok1 {
		if ok2 {
			// Both are errors
			return "", reverse, ErrNoPattern
		}
		// ret1 is error. ret2 is string
		return ret2.(string), reverse, nil
	}
	if ok2 {
		// ret2 is error. ret1 is string
		return ret1.(string), reverse, nil
	}
	// Both are strings
	return ret2.(string), reverse, nil
}
