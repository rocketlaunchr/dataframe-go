// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package interpolation

import (
	"context"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// interpolateMethod is the algorithm used to interpolate.
// Keep unexported!
type interpolateMethod interface {
	x() // Keep unexported!
}

// ForwardFill will fill nil values using the actual value on the left side of
// a segment of nil values.
type ForwardFill struct{}

func (m ForwardFill) x() {}

// BackwardFill will fill nil values using the actual value on the right side of
// a segment of nil values.
type BackwardFill struct{}

func (m BackwardFill) x() {}

// Linear will fill nil values using a straight line between the actual values of a segment
// of nil values.
type Linear struct{}

func (m Linear) x() {}

// Spline will fill nil values using the spline algorithm.
// Currently only Cubic is supported.
type Spline struct {

	// Order must be 3 for now.
	Order int
}

func (m Spline) x() {}

// Lagrange will fill nil values using the Lagrange interpolation algorithm.
// It can not be used to extrapolate.
//
// See: http://mathworld.wolfram.com/LagrangeInterpolatingPolynomial.html
type Lagrange struct {

	// Order is not implemented.
	Order int
}

func (m Lagrange) x() {}

func fill(ctx context.Context, fillFn func(int) (float64, error), fs *dataframe.SeriesFloat64, omap *dataframe.OrderedMapIntFloat64, start, end int, dir FillDirection, limit *int) error {

	if end-start <= 1 {
		return nil
	}

	var added int

	Len := end - start - 1

	if dir.has(Forward) && dir.has(Backward) {

		for j := 0; j < Len; j++ {

			if err := ctx.Err(); err != nil {
				return err
			}

			var idx int
			if j%2 == 0 {
				idx = j / 2
			} else {
				idx = Len - (1+j)/2
			}

			y, err := fillFn(j)
			if err != nil {
				return err
			}
			if omap != nil {
				omap.Set(start+1+idx, y)
			} else {
				fs.Update(start+1+idx, y, dataframe.DontLock)
			}
			added++

			if limit != nil && added >= *limit {
				return nil
			}

		}

	} else if dir.has(Forward) || dir == 0 {

		for j := 0; j < Len; j++ {

			if err := ctx.Err(); err != nil {
				return err
			}

			y, err := fillFn(j)
			if err != nil {
				return err
			}
			if omap != nil {
				omap.Set(start+1+j, y)
			} else {
				fs.Update(start+1+j, y, dataframe.DontLock)
			}
			added++

			if limit != nil && added >= *limit {
				return nil
			}
		}

	} else if dir.has(Backward) {

		for j := Len - 1; j >= 0; j-- {

			if err := ctx.Err(); err != nil {
				return err
			}

			y, err := fillFn(j)
			if err != nil {
				return err
			}
			if omap != nil {
				omap.Set(start+1+j, y)
			} else {
				fs.Update(start+1+j, y, dataframe.DontLock)
			}
			added++

			if limit != nil && added >= *limit {
				return nil
			}
		}

	}

	return nil
}

func xVal(row int, fs *dataframe.SeriesFloat64, xaxisF *dataframe.SeriesFloat64, xaxisT *dataframe.SeriesTime, start int) float64 {

	if xaxisF == nil && xaxisT == nil {
		return float64(row)
	}

	if xaxisF != nil {
		// SeriesFloat64
		if len(fs.Values) == len(xaxisF.Values) {
			return xaxisF.Values[row]
		}

		return xaxisF.Values[row-start]
	}

	// SeriesTime (Special case)
	if len(fs.Values) == len(xaxisT.Values) {
		t := xaxisT.Values[row].UnixNano()
		return float64(t / 1000) // Change time from nanoseconds to microseconds
	}

	t := xaxisT.Values[row-start].UnixNano()
	return float64(t / 1000) // Change time from nanoseconds to microseconds
}
