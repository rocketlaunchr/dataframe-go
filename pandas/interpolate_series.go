// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package pandas

import (
	"context"
	"errors"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func interpolateSeriesFloat64(ctx context.Context, fs *dataframe.SeriesFloat64, opts InterpolateOptions) (*dataframe.OrderedMapIntFloat64, error) {

	if !opts.DontLock {
		fs.Lock()
		defer fs.Unlock()
	}

	var omap *dataframe.OrderedMapIntFloat64
	if !opts.InPlace {
		omap = dataframe.NewOrderedMapIntFloat64()
	}

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

	// Step 1: Find ranges that are nil values in between

	for {

		if err := ctx.Err(); err != nil {
			return nil, err
		}

		var (
			left  *int
			right *int
		)

		for i := start; i <= end; i++ {
			currentVal := s.Values[i]
			if !math.IsNaN(currentVal) {
				if left == nil {
					left = &[]int{i}[0]
				} else {
					right = &[]int{i}[0]
					break
				}
			}
		}

		if left != nil && right != nil {
			if opts.LimitArea == nil || opts.LimitArea.has(Inner) {
				// Fill Inner range

				switch opts.Method {
				case ForwardFill:
					fillFn := func(row int) float64 {
						return fs.Values[*left]
					}
					err := fill(ctx, fillFn, fs, omap, *left, *right, opts.LimitDirection, opts.Limit)
					if err != nil {
						return nil, err
					}
				case BackwardFill:
					fillFn := func(row int) float64 {
						return fs.Values[*right]
					}
					err := fill(ctx, fillFn, fs, omap, *left, *right, opts.LimitDirection, opts.Limit)
					if err != nil {
						return nil, err
					}
				case Linear:
					grad := (fs.Values[*right] - fs.Values[*left]) / (*right - *left)
					c := fs.Values[*left] + grad
					fillFn := func(row int) float64 {
						return grad*row + (fs.Values[*left] + grad)
					}
					err := fill(ctx, fillFn, fs, omap, *left, *right, opts.LimitDirection, opts.Limit)
					if err != nil {
						return nil, err
					}
				}

			}
		} else {
			break
		}

	}

	// if mthd == ForwardFill {
	// 	// call forward fill function
	// 	err := forwardFill(ctx, fs, start, end, lim, limDir, limArea)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// } else if mthd == BackwardFill {
	// 	// call backward fill function
	// 	err := backwardFill(ctx, fs, start, end, lim, limDir, limArea)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// } else if mthd == Linear {
	// 	// call linear function

	// 	err := linearFill(ctx, fs, start, end, lim, limDir, limArea)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// } else {
	// 	return nil, errors.New("the specified interpolation method is not available")
	// }

	if opts.InPlace {
		return nil, nil
	} else {
		return omap, nil
	}
}
