// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package forecast

import (
	"context"
	"math"

	"github.com/cnkei/gospline"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func interpolateSeriesFloat64(ctx context.Context, fs *dataframe.SeriesFloat64, opts InterpolateOptions) (*dataframe.OrderedMapIntFloat64, error) {

	if !opts.DontLock {
		fs.Lock()
		defer fs.Unlock()
	}

	var xaxis *dataframe.SeriesFloat64
	if opts.XAxis != nil {
		switch s := opts.XAxis.(type) {
		case *dataframe.SeriesFloat64:
			xaxis = s
		case dataframe.ToSeriesFloat64:
			var err error
			xaxis, err = s.ToSeriesFloat64(ctx, false)
			if err != nil {
				return nil, err
			}
		default:
			panic("XAxis option must be a SeriesFloat64 or convertable to a SeriesFloat64")
		}
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

	if xaxis != nil {

		subsetL := end - start + 1

		if len(xaxis.Values) != len(fs.Values) && len(xaxis.Values) != subsetL {
			panic("XAxis must contain the same number of rows")
		}

		// TODO: When !InPlace, check at the location of algorithm.
		if opts.InPlace {

			ncOpts := dataframe.NilCountOptions{
				Ctx:          ctx,
				R:            &dataframe.Range{Start: &start, End: &end},
				DontLock:     true,
				StopAtOneNil: true,
			}

			nc, err := xaxis.NilCount(ncOpts)
			if err != nil {
				return nil, err
			}

			if nc > 0 {
				panic("XAxis must contain the no nil values")
			}
		}
	}

	// TODO: Check if there is only 1 non-nil value between start and end.

	var (
		startOfSeg int = start

		firstRow *int // row of first known value
		lastRow  *int // row of last known value
	)

	//////// FOR SPLINE INTERPOLATION //////

	var spline gospline.Spline

	switch method := opts.Method.(type) {
	case Spline:
		if method.Order == 3 {
			// requires at least 2 known values

			xVals := []float64{}
			yVals := []float64{}

			for x := startOfSeg; x <= end; x++ {
				y := fs.Values[x]
				if !math.IsNaN(y) {
					xVals = append(xVals, float64(x))
					yVals = append(yVals, y)
				}
			}

			spline = gospline.NewCubicSpline(xVals, yVals)
		}
	}

	////////////////////////////////////////

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

				if firstRow == nil {
					firstRow = &[]int{i}[0]
				}

				if left == nil {
					left = &[]int{i}[0]
				} else {
					right = &[]int{i}[0]
					lastRow = &[]int{i}[0]
					break
				}
			}
		}

		if left != nil && right != nil {
			if opts.FillRegion == nil || opts.FillRegion.has(Interpolation) {
				// Fill Inner range

				switch method := opts.Method.(type) {
				case nil, ForwardFill:
					fillFn := func(row int) float64 {
						return fs.Values[*left]
					}
					err := fill(ctx, fillFn, fs, omap, *left, *right, opts.FillDirection, opts.Limit)
					if err != nil {
						return nil, err
					}
				case BackwardFill:
					fillFn := func(row int) float64 {
						return fs.Values[*right]
					}
					err := fill(ctx, fillFn, fs, omap, *left, *right, opts.FillDirection, opts.Limit)
					if err != nil {
						return nil, err
					}
				case Linear:
					grad := (fs.Values[*right] - fs.Values[*left]) / float64(*right-*left)
					c := fs.Values[*left] + grad
					fillFn := func(row int) float64 {
						return grad*float64(row) + c
					}
					err := fill(ctx, fillFn, fs, omap, *left, *right, opts.FillDirection, opts.Limit)
					if err != nil {
						return nil, err
					}
				case Spline:
					if method.Order == 3 {
						splineVals := spline.Range(float64(*left), float64(*right), 1)
						fillFn := func(row int) float64 {
							return splineVals[row+1]
						}
						err := fill(ctx, fillFn, fs, omap, *left, *right, opts.FillDirection, opts.Limit)
						if err != nil {
							return nil, err
						}
					}
				}
			}
			startOfSeg = *right
		} else {
			break
		}
	}

	// Extrapolation
	if opts.FillRegion == nil || opts.FillRegion.has(Extrapolation) {

		// Left side
		if start != *firstRow {
			switch method := opts.Method.(type) {
			case nil, ForwardFill, BackwardFill:
				fillFn := func(row int) float64 {
					return fs.Values[*firstRow]
				}
				err := fill(ctx, fillFn, fs, omap, start-1, *firstRow, opts.FillDirection, opts.Limit)
				if err != nil {
					return nil, err
				}
			case Linear:
				var grad float64
				if omap != nil {
					y1 := fs.Values[*firstRow] // existing value
					y2, exists := omap.Get(*firstRow + 1)
					if !exists {
						y2 = fs.Values[*firstRow+1]
					}
					grad = (y2 - y1) / 1.0
				} else {
					grad = (fs.Values[*firstRow+1] - fs.Values[*firstRow]) / 1.0
				}
				c := fs.Values[*firstRow] - grad*float64(*firstRow)

				fillFn := func(row int) float64 {
					return grad*float64(row+start) + c
				}
				err := fill(ctx, fillFn, fs, omap, start-1, *firstRow, opts.FillDirection, opts.Limit)
				if err != nil {
					return nil, err
				}
			case Spline:
				if method.Order == 3 {
					splineVals := spline.Range(float64(start-1), float64(*firstRow), 1)
					fillFn := func(row int) float64 {
						return splineVals[row+1]
					}
					err := fill(ctx, fillFn, fs, omap, start-1, *firstRow, opts.FillDirection, opts.Limit)
					if err != nil {
						return nil, err
					}
				}
			}
		}

		// Right side
		if end != *lastRow {
			switch method := opts.Method.(type) {
			case nil, ForwardFill, BackwardFill:
				fillFn := func(row int) float64 {
					return fs.Values[*lastRow]
				}
				err := fill(ctx, fillFn, fs, omap, *lastRow, end+1, opts.FillDirection, opts.Limit)
				if err != nil {
					return nil, err
				}
			case Linear:
				var grad float64
				if omap != nil {
					y2 := fs.Values[*lastRow] // existing value
					y1, exists := omap.Get(*lastRow - 1)
					if !exists {
						y1 = fs.Values[*lastRow-1]
					}
					grad = (y2 - y1) / 1.0
				} else {
					grad = (fs.Values[*lastRow] - fs.Values[*lastRow-1]) / 1.0
				}
				c := fs.Values[*lastRow] - grad*float64(*lastRow)

				fillFn := func(row int) float64 {
					return grad*float64(row+*lastRow+1) + c
				}
				err := fill(ctx, fillFn, fs, omap, *lastRow, end+1, opts.FillDirection, opts.Limit)
				if err != nil {
					return nil, err
				}
			case Spline:
				if method.Order == 3 {
					splineVals := spline.Range(float64(*lastRow), float64(end+1), 1)
					fillFn := func(row int) float64 {
						return splineVals[row+1]
					}
					err := fill(ctx, fillFn, fs, omap, *lastRow, end+1, opts.FillDirection, opts.Limit)
					if err != nil {
						return nil, err
					}
				}
			}
		}

	}

	if opts.InPlace {
		return nil, nil
	}
	return omap, nil
}
