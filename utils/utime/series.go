// Copyright 2019-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package utime

import (
	"context"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// NewSeriesTimeOptions sets how NewSeriesTime decides at which row to stop.
type NewSeriesTimeOptions struct {

	// Size determines how many rows are generated.
	// This option can't be used with Until option.
	Size *int

	// Until is the maximum time in the generated Series.
	// This option can't be used with Size option.
	Until *time.Time
}

// NewSeriesTime will create a new SeriesTime with timeFreq prescribing the intervals between each row. Setting reverse will make the time series decrement per row.
//
// See https://godoc.org/github.com/rocketlaunchr/dataframe-go/utils/utime#TimeIntervalGenerator for setting timeFreq.
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
	var times []*time.Time
	if opts.Size != nil {
		times = make([]*time.Time, 0, *opts.Size)
	} else {
		times = []*time.Time{}
	}

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

		times = append(times, &nt)
	}

	st := dataframe.NewSeriesTime(name, nil)
	st.Values = times

	return st, nil
}
