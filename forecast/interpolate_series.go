// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package forecast

import (
	"context"
	"math"

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

	r := &dataframe.Range{}
	if opts.R != nil {
		r = opts.R
	}

	start, end, err := r.Limits(len(fs.Values))
	if err != nil {
		return nil, err
	}

	startOfSeg := start

	// Step 1: Find ranges that are nil values in between

	for {

		if startOfSeg >= end-1 {
			break
		}

		if err := ctx.Err(); err != nil {
			return nil, err
		}

		var (
			left  *int
			right *int
		)

		for i := startOfSeg; i <= end; i++ {
			currentVal := fs.Values[i]
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
					grad := (fs.Values[*right] - fs.Values[*left]) / float64(*right-*left)
					c := fs.Values[*left] + grad
					fillFn := func(row int) float64 {
						return grad*float64(row) + c
					}
					err := fill(ctx, fillFn, fs, omap, *left, *right, opts.LimitDirection, opts.Limit)
					if err != nil {
						return nil, err
					}
				}

			}
			startOfSeg = *right
		} else {
			break
		}
	}
	// Extrapolating Outer values
	// for Nan values at start edge assign the nearest valid value fwd to the right
	// for nan values at the end edge assign the nearest valid value bkwd to the left
	// https://github.com/pandas-dev/pandas/issues/16284#issuecomment-303132712
	if opts.LimitArea == nil || opts.LimitArea.has(Outer) {
		var (
			fillVal float64
			cnt     int
		)

		// checking edge from start for consecutive Na values
		if math.IsNaN(fs.Values[start]) {
			cnt = 1
			for i := start; i <= end; i++ {
				if !math.IsNaN(fs.Values[i]) {
					fillVal = fs.Values[i]

					// fill nans
					for j := 0; j < cnt; j++ {
						fs.Values[start+j] = fillVal
					}
					break
				}
				cnt++
			}

		}
		// checking edge from end for consecutive Na values
		if math.IsNaN(fs.Values[end]) {
			cnt = 1
			for i := end; i >= start; i-- {
				if !math.IsNaN(fs.Values[i]) {
					fillVal = fs.Values[i]

					// fill nans
					for j := 0; j < cnt; j++ {
						fs.Values[end-j] = fillVal
					}
					break
				}
				cnt++
			}

		}
	}

	if opts.InPlace {
		return nil, nil
	}
	return omap, nil
}
