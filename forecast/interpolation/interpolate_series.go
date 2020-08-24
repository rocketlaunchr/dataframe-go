// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package interpolation

import (
	"context"
	"golang.org/x/xerrors"
	"math"

	"github.com/DzananGanic/numericalgo/interpolate"
	"github.com/DzananGanic/numericalgo/interpolate/lagrange"
	"github.com/cnkei/gospline"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/utils"
)

func interpolateSeriesFloat64(ctx context.Context, fs *dataframe.SeriesFloat64, opts InterpolateOptions) (*dataframe.OrderedMapIntFloat64, error) {

	if !opts.DontLock {
		fs.Lock()
		defer fs.Unlock()
	}

	var (
		xaxisF *dataframe.SeriesFloat64
		xaxisT *dataframe.SeriesTime
	)

	if opts.HorizAxis != nil {
		switch s := opts.HorizAxis.(type) {
		case *dataframe.SeriesFloat64:
			xaxisF = s
		case *dataframe.SeriesTime:
			xaxisT = s
		case dataframe.ToSeriesFloat64:
			var err error
			xaxisF, err = s.ToSeriesFloat64(ctx, false)
			if err != nil {
				return nil, err
			}
		default:
			panic("HorizAxis option must be a SeriesFloat64/SeriesTime or convertable to a SeriesFloat64")
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

	if xaxisF != nil || xaxisT != nil {

		subsetL := end - start + 1

		if xaxisF != nil {
			if len(xaxisF.Values) != len(fs.Values) && len(xaxisF.Values) != subsetL {
				panic("HorizAxis must contain the same number of rows")
			}
		} else {
			if len(xaxisT.Values) != len(fs.Values) && len(xaxisT.Values) != subsetL {
				panic("HorizAxis must contain the same number of rows")
			}
		}

		if opts.InPlace {

			ncOpts := dataframe.NilCountOptions{
				Ctx:          ctx,
				R:            &dataframe.Range{Start: &start, End: &end},
				DontLock:     true,
				StopAtOneNil: true,
			}

			var (
				nc  int
				err error
			)

			if xaxisF != nil {
				nc, err = xaxisF.NilCount(ncOpts)
				if err != nil {
					return nil, err
				}
			} else {
				nc, err = xaxisT.NilCount(ncOpts)
				if err != nil {
					return nil, err
				}
			}

			if nc > 0 {
				panic("HorizAxis must contain no nil values")
			}
		}
	}

	// TODO: Check if there is only 1 non-nil value between start and end.

	var (
		startOfSeg int = start

		firstRow *int // row of first known value
		lastRow  *int // row of last known value
	)

	//////// FOR ALGORITHM PREPARATION //////

	var alg interface{}

	switch method := opts.Method.(type) {
	case Spline:
		if method.Order == 3 {
			// requires at least 2 known values

			xVals := []float64{}
			yVals := []float64{}

			for x := start; x <= end; x++ {
				y := fs.Values[x]
				if !math.IsNaN(y) {
					xVal := xVal(x, fs, xaxisF, xaxisT, start)
					if math.IsNaN(xVal) {
						panic("HorizAxis must contain no nil values")
					}
					xVals = append(xVals, xVal)
					yVals = append(yVals, y)
				}
			}

			alg = gospline.NewCubicSpline(xVals, yVals)
		}
	case Lagrange:

		xVals := []float64{}
		yVals := []float64{}

		for x := start; x <= end; x++ {
			y := fs.Values[x]
			if !math.IsNaN(y) {
				xVal := xVal(x, fs, xaxisF, xaxisT, start)
				if math.IsNaN(xVal) {
					panic("HorizAxis must contain no nil values")
				}
				xVals = append(xVals, xVal)
				yVals = append(yVals, y)
			}
		}

		li := lagrange.New()
		li.Fit(xVals, yVals)

		alg = li
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
					fillFn := func(row int) (float64, error) {
						return fs.Values[*left], nil
					}
					err := fill(ctx, fillFn, fs, omap, *left, *right, opts.FillDirection, opts.Limit)
					if err != nil {
						return nil, err
					}
				case BackwardFill:
					fillFn := func(row int) (float64, error) {
						return fs.Values[*right], nil
					}
					err := fill(ctx, fillFn, fs, omap, *left, *right, opts.FillDirection, opts.Limit)
					if err != nil {
						return nil, err
					}
				case Linear:
					var fillFn func(int) (float64, error)
					if xaxisF == nil && xaxisT == nil {
						grad := (fs.Values[*right] - fs.Values[*left]) / float64(*right-*left)
						c := fs.Values[*left] + grad
						fillFn = func(row int) (float64, error) {
							return grad*float64(row) + c, nil
						}
					} else {
						xLeft := xVal(*left, fs, xaxisF, xaxisT, start)
						if math.IsNaN(xLeft) {
							panic("HorizAxis must contain no nil values")
						}
						xRight := xVal(*right, fs, xaxisF, xaxisT, start)
						if math.IsNaN(xRight) {
							panic("HorizAxis must contain no nil values")
						}
						grad := (fs.Values[*right] - fs.Values[*left]) / (xRight - xLeft)
						fillFn = func(row int) (float64, error) {
							xr := xVal(*left+row+1, fs, xaxisF, xaxisT, start)
							if math.IsNaN(xr) {
								panic("HorizAxis must contain no nil values")
							}
							Δx := xr - xLeft
							return grad*Δx + fs.Values[*left], nil
						}
					}

					err := fill(ctx, fillFn, fs, omap, *left, *right, opts.FillDirection, opts.Limit)
					if err != nil {
						return nil, err
					}
				case Spline:
					if method.Order == 3 {
						var fillFn func(int) (float64, error)
						if xaxisF == nil && xaxisT == nil {
							splineVals := alg.(gospline.Spline).Range(float64(*left), float64(*right), 1)
							fillFn = func(row int) (float64, error) {
								return splineVals[row+1], nil
							}
						} else {
							fillFn = func(row int) (float64, error) {
								xr := xVal(*left+row+1, fs, xaxisF, xaxisT, start)
								return alg.(gospline.Spline).At(xr), nil
							}
						}

						err := fill(ctx, fillFn, fs, omap, *left, *right, opts.FillDirection, opts.Limit)
						if err != nil {
							return nil, err
						}
					}
				case Lagrange:
					var fillFn func(int) (float64, error)
					if xaxisF == nil && xaxisT == nil {
						lagrangeXVals := utils.Float64Range(float64(*left), float64(*right), 1)
						lagrangeYVals, err := interpolate.WithMulti(alg.(*lagrange.Lagrange), lagrangeXVals)
						if err != nil {
							return nil, xerrors.Errorf("Lagrange method: %w", err)
						}
						fillFn = func(row int) (float64, error) {
							return lagrangeYVals[row+1], nil
						}
					} else {
						fillFn = func(row int) (float64, error) {
							xr := xVal(*left+row+1, fs, xaxisF, xaxisT, start)
							lagrangeYVal, err := interpolate.WithSingle(alg.(*lagrange.Lagrange), xr)
							if err != nil {
								return 0, xerrors.Errorf("Lagrange method: %w", err)
							}
							return lagrangeYVal, nil
						}
					}

					err = fill(ctx, fillFn, fs, omap, *left, *right, opts.FillDirection, opts.Limit)
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

	// Extrapolation
	if opts.FillRegion == nil || opts.FillRegion.has(Extrapolation) {

		// Left side
		if start != *firstRow {
			switch method := opts.Method.(type) {
			case nil, ForwardFill, BackwardFill:
				fillFn := func(row int) (float64, error) {
					return fs.Values[*firstRow], nil
				}
				err := fill(ctx, fillFn, fs, omap, start-1, *firstRow, opts.FillDirection, opts.Limit)
				if err != nil {
					return nil, err
				}
			case Linear:
				var fillFn func(int) (float64, error)

				if xaxisF == nil && xaxisT == nil {
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

					fillFn = func(row int) (float64, error) {
						return grad*float64(row+start) + c, nil
					}
				} else {
					// Calculate gradient
					xFirstRow := xVal(*firstRow, fs, xaxisF, xaxisT, start)
					if math.IsNaN(xFirstRow) {
						panic("HorizAxis must contain no nil values")
					}
					xFirstRowPlusOne := xVal(*firstRow+1, fs, xaxisF, xaxisT, start)
					if math.IsNaN(xFirstRowPlusOne) {
						panic("HorizAxis must contain no nil values")
					}
					Δx := xFirstRowPlusOne - xFirstRow

					var grad float64
					if omap != nil {
						y1 := fs.Values[*firstRow] // existing value
						y2, exists := omap.Get(*firstRow + 1)
						if !exists {
							y2 = fs.Values[*firstRow+1]
						}
						grad = (y2 - y1) / Δx
					} else {
						grad = (fs.Values[*firstRow+1] - fs.Values[*firstRow]) / Δx
					}

					fillFn = func(row int) (float64, error) {
						xr := xVal(start+row, fs, xaxisF, xaxisT, start)
						if math.IsNaN(xr) {
							panic("HorizAxis must contain no nil values")
						}
						Δx := xFirstRow - xr
						return fs.Values[*firstRow] - grad*Δx, nil
					}
				}

				err := fill(ctx, fillFn, fs, omap, start-1, *firstRow, opts.FillDirection, opts.Limit)
				if err != nil {
					return nil, err
				}
			case Spline:
				if method.Order == 3 {
					splineVals := alg.(gospline.Spline).Range(float64(start-1), float64(*firstRow), 1)
					fillFn := func(row int) (float64, error) {
						return splineVals[row+1], nil
					}
					err := fill(ctx, fillFn, fs, omap, start-1, *firstRow, opts.FillDirection, opts.Limit)
					if err != nil {
						return nil, err
					}
				}
			case Lagrange:
				// Can't be used to extrapolate.
				// Package returns error: Value to interpolate is too small and not in range
			}
		}

		// Right side
		if end != *lastRow {
			switch method := opts.Method.(type) {
			case nil, ForwardFill, BackwardFill:
				fillFn := func(row int) (float64, error) {
					return fs.Values[*lastRow], nil
				}
				err := fill(ctx, fillFn, fs, omap, *lastRow, end+1, opts.FillDirection, opts.Limit)
				if err != nil {
					return nil, err
				}
			case Linear:
				var fillFn func(int) (float64, error)

				if xaxisF == nil && xaxisT == nil {
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

					fillFn = func(row int) (float64, error) {
						return grad*float64(row+*lastRow+1) + c, nil
					}
				} else {
					// Calculate gradient
					xLastRow := xVal(*lastRow, fs, xaxisF, xaxisT, start)
					if math.IsNaN(xLastRow) {
						panic("HorizAxis must contain no nil values")
					}
					xLastRowMinusOne := xVal(*lastRow-1, fs, xaxisF, xaxisT, start)
					if math.IsNaN(xLastRowMinusOne) {
						panic("HorizAxis must contain no nil values")
					}
					Δx := xLastRow - xLastRowMinusOne

					var grad float64
					if omap != nil {
						y2 := fs.Values[*lastRow] // existing value
						y1, exists := omap.Get(*lastRow - 1)
						if !exists {
							y1 = fs.Values[*lastRow-1]
						}
						grad = (y2 - y1) / Δx
					} else {
						grad = (fs.Values[*lastRow] - fs.Values[*lastRow-1]) / Δx
					}

					fillFn = func(row int) (float64, error) {
						xr := xVal(*lastRow+1+row, fs, xaxisF, xaxisT, start)
						if math.IsNaN(xr) {
							panic("HorizAxis must contain no nil values")
						}
						Δx := xr - xLastRow
						return grad*Δx + fs.Values[*lastRow], nil
					}
				}

				err := fill(ctx, fillFn, fs, omap, *lastRow, end+1, opts.FillDirection, opts.Limit)
				if err != nil {
					return nil, err
				}
			case Spline:
				if method.Order == 3 {
					splineVals := alg.(gospline.Spline).Range(float64(*lastRow), float64(end+1), 1)
					fillFn := func(row int) (float64, error) {
						return splineVals[row+1], nil
					}
					err := fill(ctx, fillFn, fs, omap, *lastRow, end+1, opts.FillDirection, opts.Limit)
					if err != nil {
						return nil, err
					}
				}
			case Lagrange:
				// Can't be used to extrapolate.
				// Package returns error: Value to interpolate is too small and not in range
			}
		}

	}

	if opts.InPlace {
		return nil, nil
	}
	return omap, nil
}
