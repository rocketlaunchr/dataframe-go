// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package pandas

import (
	"context"
	"errors"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func interpolateSeriesFloat64(ctx context.Context, fs *dataframe.SeriesFloat64, opts InterpolateOptions) (*dataframe.OrderedMapIntFloat64, error) {

	// if !opts.DontLock {
	// 	fs.Lock()
	// 	defer fs.Unlock()
	// }

	var (
		mthd    InterpolateMethod
		lim     int
		limDir  InterpolationLimitDirection
		limArea InterpolationLimitArea
		// fsc     *dataframe.SeriesFloat64
	)

	mthd = opts.Method
	if opts.Limit != nil && *opts.Limit > 0 {
		lim = *opts.Limit
	} else {
		lim = fs.NilCount(dataframe.DontLock) // set to max number of nilCount
	}

	limDir = opts.LimitDirection
	limArea = opts.LimitArea

	// fsc = fs.Copy().(*dataframe.SeriesFloat64) // make a copy of series and work with copy

	if mthd == ForwardFill {
		// call forward fill function
		res, err := forwardFill(ctx, fs, limDir, limArea, lim, opts.R)
		if err != nil {
			return nil, err
		}
		_ = res

	} else if mthd == BackwardFill {
		// call backward fill function
		res, err := backwardFill(ctx, fs, limDir, limArea, lim, opts.R)
		if err != nil {
			return nil, err
		}
		_ = res

	} else if mthd == Linear {
		// call linear function

		res, err := backwardFill(ctx, fs, limDir, limArea, lim, opts.R)
		if err != nil {
			return nil, err
		}
		_ = res
	} else {
		return nil, errors.New("the specified interpolation method is not available")
	}

	if opts.InPlace {
		// return res, nil
	} else {
		//  return OrderedMapIntFloat64
	}

	return nil, nil
}
