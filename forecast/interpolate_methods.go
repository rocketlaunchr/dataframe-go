// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package forecast

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
// Currently not implemented.
//
// See http://mathworld.wolfram.com/LagrangeInterpolatingPolynomial.html
// and https://github.com/DzananGanic/numericalgo
type Lagrange struct {

	// Order is not implemented.
	Order int
}

func (m Lagrange) x() {}

func fill(ctx context.Context, fillFn func(int) float64, fs *dataframe.SeriesFloat64, omap *dataframe.OrderedMapIntFloat64, start, end int, dir InterpolationFillDirection, limit *int) error {

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

			if omap != nil {
				omap.Set(start+1+idx, fillFn(j))
			} else {
				fs.Update(start+1+idx, fillFn(j), dataframe.DontLock)
			}
			added++

			if limit != nil && added >= *limit {
				return nil
			}

		}

	} else if dir.has(Forward) {

		for j := 0; j < Len; j++ {

			if err := ctx.Err(); err != nil {
				return err
			}

			if omap != nil {
				omap.Set(start+1+j, fillFn(j))
			} else {
				fs.Update(start+1+j, fillFn(j), dataframe.DontLock)
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

			if omap != nil {
				omap.Set(start+1+j, fillFn(j))
			} else {
				fs.Update(start+1+j, fillFn(j), dataframe.DontLock)
			}
			added++

			if limit != nil && added >= *limit {
				return nil
			}
		}

	}

	return nil
}
