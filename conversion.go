// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"context"
)

// ToSeriesInt64 is an interface used by the Dataframe to know if a particular
// Series can be converted to a SeriesInt64 Series.
type ToSeriesInt64 interface {

	// ToSeriesInt64 is used to convert a particular Series to a SeriesInt64.
	// If the returned Series is not nil but an error is still provided,
	// it means that some rows were not able to be converted. You can inspect
	// the error to determine which rows were unconverted.
	//
	// NOTE: The returned ErrorCollection should contain RowError objects.
	ToSeriesInt64(context.Context, bool, ...func(interface{}) (*int64, error)) (*SeriesInt64, error)
}

// ToSeriesString is an interface used by the Dataframe to know if a particular
// Series can be converted to a SeriesString Series.
type ToSeriesString interface {

	// ToSeriesString is used to convert a particular Series to a SeriesString.
	// If the returned Series is not nil but an error is still provided,
	// it means that some rows were not able to be converted. You can inspect
	// the error to determine which rows were unconverted.
	//
	// NOTE: The returned ErrorCollection should contain RowError objects.
	ToSeriesString(context.Context, bool, ...func(interface{}) (*string, error)) (*SeriesString, error)
}

// ToSeriesFloat64 is an interface used by the Dataframe to know if a particular
// Series can be converted to a SeriesFloat64 Series.
type ToSeriesFloat64 interface {

	// ToSeriesFloat64 is used to convert a particular Series to a SeriesFloat64.
	// If the returned Series is not nil but an error is still provided,
	// it means that some rows were not able to be converted. You can inspect
	// the error to determine which rows were unconverted.
	//
	// NOTE: The returned ErrorCollection should contain RowError objects.
	ToSeriesFloat64(context.Context, bool, ...func(interface{}) (float64, error)) (*SeriesFloat64, error)
}
