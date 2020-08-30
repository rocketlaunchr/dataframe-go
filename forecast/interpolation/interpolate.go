// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

// Package interpolation implements various algorithms to fill in missing values in a Series or DataFrame.
package interpolation

import (
	"context"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// FillDirection is used to set the direction that nil values are filled.
type FillDirection uint8

func (opt FillDirection) has(x FillDirection) bool {
	return opt&x != 0
}

const (

	// Forward interpolates nil values from left to right.
	Forward FillDirection = 1 << iota

	// Backward interpolates nil values from right to left.
	Backward
)

// FillRegion is used to set the fill region.
type FillRegion uint8

func (opt FillRegion) has(x FillRegion) bool {
	return opt&x != 0
}

const (

	// Interpolation estimates values between two known values.
	Interpolation FillRegion = 1 << iota

	// Extrapolation estimates values by extending a known sequence of values beyond
	// what is certainly known.
	Extrapolation
)

// InterpolateOptions is used to configure the Interpolate function.
type InterpolateOptions struct {

	// Method sets the algorithm used to interpolate.
	// Current options are: ForwardFill{} (default), BackwardFill{}, Linear{}, Spline{} and Lagrange{}.
	Method interpolateMethod

	// Limit sets the maximum number of consecutive nil values to fill.
	// The default is unlimited, but if set, it must be greater than 0.
	Limit *int

	// FillDirection sets the direction that nil values are interpolated.
	// The default is Forward.
	FillDirection FillDirection

	// FillRegion sets whether the interpolation function should fill nil values by interpolating and/or extrapolating.
	// The default is both.
	FillRegion *FillRegion

	// InPlace will perform the interpolation operation on the current SeriesFloat64 or DataFrame.
	// If InPlace is not set, an OrderedMapIntFloat64 will be returned. The original Series or DataFrame will be unmodified.
	InPlace bool

	// DontLock can be set to true if the Series or DataFrame should not be locked.
	DontLock bool

	// R is used to limit the range of the Series for interpolation purposes.
	R *dataframe.Range

	// HorizAxis is used to set the "x-axis" for the purposes of interpolation.
	// If not set, the horizontal axis is deemed to be spaced out with units of 1.
	// It must implement a dataframe.ToSeriesFloat64 or be a SeriesFloat64/SeriesTime.
	// It must not contain nil values in the range R.
	// When used with a DataFrame, it may be an int or string to identify the Series of the DataFrame to be used
	// as the horizontal axis.
	HorizAxis interface{}
}

// Interpolate will accept a DataFrame or SeriesFloat64 and interpolate the missing values.
// If the InPlace option is set, the DataFrame or SeriesFloat64 is modified "in place".
// Alternatively, a map[interface{}]*dataframe.OrderedMapIntFloat64 or *dataframe.OrderedMapIntFloat64 is returned respectively.
// When used with a DataFrame, only SeriesFloat64 columns (that are not set as the HorizAxis) are interpolated.
func Interpolate(ctx context.Context, sdf interface{}, opts InterpolateOptions) (interface{}, error) {

	switch typ := sdf.(type) {
	case *dataframe.SeriesFloat64:
		x, err := interpolateSeriesFloat64(ctx, typ, opts)
		if err != nil {
			return nil, err
		}
		return x, err
	case *dataframe.DataFrame:
		x, err := interpolateDataFrame(ctx, typ, opts)
		if err != nil {
			return nil, err
		}
		return x, err
	default:
		panic("sdf must be a SeriesFloat64 or DataFrame")
	}

	return nil, nil
}
