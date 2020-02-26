// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package forecast

import (
	"context"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

type InterpolationFillDirection uint8

func (opt InterpolationFillDirection) has(x InterpolationFillDirection) bool {
	return opt&x != 0
}

const (

	// Forward interpolates nil values from left to right.
	Forward InterpolationFillDirection = 1 << iota

	// Backward interpolates nil values from right to left.
	Backward
)

type InterpolationFillRegion uint8

func (opt InterpolationFillRegion) has(x InterpolationFillRegion) bool {
	return opt&x != 0
}

const (

	// Interpolation estimates values between two known values.
	Interpolation InterpolationFillRegion = 1 << iota

	// Extrapolation estimates values by extending a known sequence of values beyond
	// what is certainly known.
	Extrapolation
)

type InterpolateOptions struct {

	// Method sets the algorithm used to interpolate.
	// The default is to ForwardFill.
	Method InterpolateMethod

	// Limit sets the maximum number of consecutive nil values to fill.
	// The default is unlimited, but if set, it must be greater than 0.
	Limit *int

	// https://www.geeksforgeeks.org/python-pandas-dataframe-interpolate/
	// https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.interpolate.html

	// FillDirection sets the direction that nil values are interpolated.
	// The default is Forward.
	FillDirection InterpolationFillDirection

	// FillRegion sets whether the interpolation function should fill nil values by interpolating and/or extrapolating.
	// The default is both.
	FillRegion *InterpolationFillRegion

	// InPlace will perform the interpolation operation on the current SeriesFloat64 or DataFrame.
	// If InPlace is not set, an OrderedMapIntFloat64 will be returned. The original Series or DataFrame will be unmodified.
	InPlace bool

	// DontLock can be set to true if the Series or DataFrame should not be locked.
	DontLock bool

	// R is used to limit the range of the Series for interpolation purposes.
	R *dataframe.Range
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
