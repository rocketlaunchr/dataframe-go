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
		lim     *int
		limDir  InterpolationLimitDirection
		limArea *InterpolationLimitArea
		r       *dataframe.Range
	)

	mthd = opts.Method
	if opts.Limit != nil && *opts.Limit > 0 {
		lim = opts.Limit
	}

	limDir = opts.LimitDirection
	if limArea != nil {
		limArea = opts.LimitArea
	}

	r = &dataframe.Range{}
	if opts.R != nil {
		r = opts.R
	}

	start, end, err := r.Limits(len(fs.Values))
	if err != nil {
		return nil, err
	}

	if mthd == ForwardFill {
		// call forward fill function
		err := forwardFill(ctx, fs, start, end, lim, limDir, limArea)
		if err != nil {
			return nil, err
		}

	} else if mthd == BackwardFill {
		// call backward fill function
		err := backwardFill(ctx, fs, start, end, lim, limDir, limArea)
		if err != nil {
			return nil, err
		}

	} else if mthd == Linear {
		// call linear function

		err := linearFill(ctx, fs, start, end, lim, limDir, limArea)
		if err != nil {
			return nil, err
		}

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
