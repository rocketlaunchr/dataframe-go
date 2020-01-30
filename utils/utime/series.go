// Copyright 2019-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package utime

import (
	"context"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

type NewSeriesTimeOptions struct {

	// Size determines how many rows are generated.
	// This option can't be used with Until option.
	Size *int

	// Until is the maximum time in the generated Series.
	// This option can't be used with Size option.
	Until *time.Time
}

func NewSeriesTime(ctx context.Context, name string, timeFreq string, startTime time.Time, reverse bool, opts NewSeriesTimeOptions) (*dataframe.SeriesTime, error) {

	if opts.Size != nil && opts.Until != nil {
		panic("Size and Until options can't be used together")
	}

	if opts.Size == nil && opts.Until == nil {
		panic("Either Size xor Until option required")
	}

	if opts.Until != nil {
		if reverse {
			if startTime.Before(*opts.Until) {
				panic("startTime must be after Until option")
			}
		} else {
			if !startTime.Before(*opts.Until) {
				panic("startTime must be before Until option")
			}
		}
	}

	// Generate time intervals.
	times := []time.Time{}

	gen, err := TimeIntervalGenerator(timeFreq)
	if err != nil {
		return nil, err
	}

	ntg := gen(startTime, reverse)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if opts.Size != nil && len(times) >= *opts.Size {
			break
		}

		nt := ntg()

		if opts.Until != nil {
			if reverse {
				if !nt.After(*opts.Until) {
					break
				}
			} else {
				if !nt.Before(*opts.Until) {
					break
				}
			}
		}

		times = append(times, nt)
	}

	return dataframe.NewSeriesTime(name, &dataframe.SeriesInit{Capacity: len(times)}, times), nil
}
