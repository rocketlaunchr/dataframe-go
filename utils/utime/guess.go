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
	wg.Add(2)

	var (
		// Could be an error or string
		ret1 interface{}
		ret2 interface{}
	)

	// Use time.Duration
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

	// Use github.com/icza/gox/timex
	go func() {
		defer wg.Done()

		var years, months, days, hours, mins, secs int

		if reverse {
			years, months, days, hours, mins, secs = timex.Diff(val1, val2)
		} else {
			years, months, days, hours, mins, secs = timex.Diff(val2, val1)
		}

		if hours != 0 || mins != 0 || secs != 0 {
			ret2 = ErrNoPattern
			return
		}

		p := parsed{years, months, 0, days}
		timeFreq := p.String()

		err := ValidateSeriesTime(ctx, ts, timeFreq, ValidateSeriesTimeOptions{DontLock: true})
		if err != nil {
			ret2 = err
		} else {
			ret2 = timeFreq
		}
	}()

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
	// TODO: return ret1 only if duration is less than 23 hours for DST purposes?
	return ret1.(string), reverse, nil
}
