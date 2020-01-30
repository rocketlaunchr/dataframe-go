// Copyright 2019-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package utime

import (
	"fmt"
	"time"
)

// NextTime will return the next time in the sequence. You can call it repeatedly
// to obtain a sequence.
type NextTime func() time.Time

// TimeGenerator will create a generator function, which when called will return
// a sequence of times. The sequence will begin at startTime. When reverse is true,
// the sequence will be backwards.
type TimeGenerator func(startTime time.Time, reverse bool) NextTime

// TimeIntervalGenerator is used to create a sequence of times based on an interval defined by
// timeFreq. timeFreq can be in the format: nYnMnWnD, where n is a non-negative integer and
// Y, M, W and D represent years, months, weeks and days respectively. Alternatively, timeFreq
// can be a valid input to time.ParseDuration.
//
// Example:
//
//  gen, _ := utime.TimeIntervalGenerator("1W1D")
//  ntg := gen(time.Now().UTC(), false)
//  for {
//     fmt.Println(ntg())
//     time.Sleep(500 * time.Millisecond)
//  }
//
// See: https://golang.org/pkg/time/#ParseDuration
func TimeIntervalGenerator(timeFreq string) (TimeGenerator, error) {

	// Prevent negative sign
	if len(timeFreq) > 0 && timeFreq[0:1] == "-" {
		return nil, fmt.Errorf("negative sign disallowed: %s", timeFreq)
	}

	var (
		d *time.Duration
		p *parsed
	)

	_d, err := time.ParseDuration(timeFreq)
	if err != nil {
		_p, err := parse(timeFreq)
		if err != nil {
			return nil, fmt.Errorf("could not parse: %s", timeFreq)
		}
		p = &[]parsed{_p}[0]
	} else {
		d = &[]time.Duration{_d}[0]
	}

	return func(startTime time.Time, reverse bool) NextTime {
		var prevTime *time.Time

		return func() time.Time {
			var nt time.Time

			if prevTime == nil {
				if d == nil {
					nt = startTime.AddDate((*p).addDate(reverse))
				} else {
					if reverse {
						nt = startTime.Add(-*d)
					} else {
						nt = startTime.Add(*d)
					}
				}
			} else {
				if d == nil {
					nt = (*prevTime).AddDate((*p).addDate(reverse))
				} else {
					if reverse {
						nt = (*prevTime).Add(-*d)
					} else {
						nt = (*prevTime).Add(*d)
					}
				}
			}
			prevTime = &[]time.Time{nt}[0]
			return nt
		}
	}, nil
}
