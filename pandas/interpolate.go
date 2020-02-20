// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package pandas

import (
	"context"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

type InterpolationLimitDirection uint8

func (opt InterpolationLimitDirection) has(x InterpolationLimitDirection) bool {
	return opt&x != 0
}

const (
	Forward InterpolationLimitDirection = 1 << iota

	Backward
)

type InterpolationLimitArea uint8

func (opt InterpolationLimitArea) has(x InterpolationLimitArea) bool {
	return opt&x != 0
}

const (
	Inner InterpolationLimitArea = 1 << iota

	Outer
)

type InterpolateMethod int

const (
	ForwardFill InterpolateMethod = 0

	// Sum will fill Nil values with the sum.
	BackwardFill InterpolateMethod = 1

	Linear InterpolateMethod = 2

	// https://github.com/cnkei/gospline
	// https://github.com/esimov/gospline
	// http://blog.ivank.net/interpolation-with-cubic-splines.html
	// http://mathworld.wolfram.com/LagrangeInterpolatingPolynomial.html
	Spline

	// http://mathworld.wolfram.com/LagrangeInterpolatingPolynomial.html
	// https://github.com/DzananGanic/numericalgo
	Lagrange
)

type InterpolateOptions struct {
	Method InterpolateMethod

	// Limit sets the maximum number of consecutive nil values to fill.
	// The default is unlimited but if set, it must be greater than 0.
	Limit *int

	// https://www.geeksforgeeks.org/python-pandas-dataframe-interpolate/
	// https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.interpolate.html
	LimitDirection InterpolationLimitDirection

	//
	LimitArea InterpolationLimitArea

	// InPlace will perform the interpolation operation on the current SeriesFloat64 or DataFrame.
	// If InPlace is not set, an OrderedMapIntFloat64 will be returned. The original Series or DataFrame will be unmodified.
	InPlace bool

	// DontLock can be set to true if the Series or DataFrame should not be locked.
	DontLock bool
}

// sdf can be a SeriesFloat64 or DataFrame.
func Interpolate(ctx context.Context, sdf interface{}, opts InterpolateOptions) (*dataframe.OrderedMapIntFloat64, error) {

	switch typ := sdf.(type) {
	case *dataframe.SeriesFloat64:
		return interpolateSeriesFloat64(ctx, typ, opts)
	case *dataframe.DataFrame:
		// return interpolateDataFrame(ctx, typ)
	default:
		panic("sdf must be a SeriesFloat64 or DataFrame")
	}

	return nil, nil
}
