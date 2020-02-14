// Copyright 2018-19 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package forecast

import (
	"context"
	"errors"
	"math"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// isInvalidFloat64 returns true if val is Inf or NaN.
func isInvalidFloat64(val float64) bool {
	if math.IsInf(val, 0) || math.IsNaN(val) {
		return true
	}
	return false
}

// ErrorType Enum is used to specify
// error type selection to use in Model Fit
type ErrorType int

const (
	// RMSE stands for Root Mean Squared Error type
	RMSE ErrorType = 0
	// SSE stands for Sum Of Squared Error type
	SSE ErrorType = 1
	// MAE stands for Mean Absolute Error type
	MAE ErrorType = 2
	// MAPE Mean Absolute percentage Error type
	MAPE ErrorType = 3
)

// ErrorMeasurement struct contains data
// about selected error type in Model Fit
type ErrorMeasurement struct {
	errorType ErrorType
	value     float64
}

// Type method returns the error measurement type in String format
func (em *ErrorMeasurement) Type() string {
	var out string

	if em.errorType == MAE {
		out = "Mean Absolute Error"
	} else if em.errorType == SSE {
		out = "Sum Of Squared Error"
	} else if em.errorType == RMSE {
		out = "Root Mean Squared Error"
	} else if em.errorType == MAPE {
		out = "Mean Absolute Percentage Error"
	}

	return out
}

// Value method returns the float64 error measurement Value
func (em *ErrorMeasurement) Value() float64 {
	return em.value
}

// ErrorOptions is used to modify the behavior of the "Error" calculation functions.
type ErrorOptions struct {

	// Don't apply lock
	DontLock bool

	// SkipInvalids will skip Inf and NaN values.
	// If set to false (default), an ErrIndeterminate will be returned.
	SkipInvalids bool
}

// MeanAbsoluteError represents the mean absolute error.
//
// See: https://otexts.com/fpp2/accuracy.html
func MeanAbsoluteError(ctx context.Context, testSeries, forecast *dataframe.SeriesFloat64, opts *ErrorOptions, r ...dataframe.Range) (float64, int, error) {

	if opts == nil || (opts != nil && !opts.DontLock) {
		// Lock both series
		testSeries.Lock()
		forecast.Lock()
		defer testSeries.Unlock()
		defer forecast.Unlock()
	}

	nPred := len(forecast.Values)

	if len(r) == 0 {
		r = append(r, dataframe.Range{Start: &[]int{-nPred}[0]})
	}

	// Check if the range of testSeries matches the range of forecast
	nTest, err := r[0].NRows(len(testSeries.Values))
	if err != nil {
		return 0.0, 0, err
	}

	if nTest != nPred {
		return 0.0, 0, ErrMismatchLen
	}

	// Calculate MAE

	start, end, err := r[0].Limits(len(testSeries.Values))
	if err != nil {
		return 0.0, 0, err
	}

	var (
		n   int
		sum float64
	)

	// Loop through testSeries starting at start.
	// Loop through forecast starting at 0.
	for i, j := start, 0; i < end+1; i, j = i+1, j+1 {

		if err := ctx.Err(); err != nil {
			return 0.0, 0, err
		}

		actual := testSeries.Values[i]
		predicted := forecast.Values[j]

		if isInvalidFloat64(actual) || isInvalidFloat64(predicted) {
			if opts != nil && opts.SkipInvalids {
				continue
			} else {
				return 0.0, 0, ErrIndeterminate
			}
		}

		e := actual - predicted

		sum = sum + math.Abs(e)
		n = n + 1
	}

	if n == 0 {
		return 0.0, 0, ErrIndeterminate
	}

	return sum / float64(n), n, nil
}

// SumOfSquaredErrors represents the sum of squared errors.
func SumOfSquaredErrors(ctx context.Context, testSeries, forecast *dataframe.SeriesFloat64, opts *ErrorOptions, r ...dataframe.Range) (float64, int, error) {

	if opts == nil || (opts != nil && !opts.DontLock) {
		// Lock both series
		testSeries.Lock()
		forecast.Lock()
		defer testSeries.Unlock()
		defer forecast.Unlock()
	}

	nPred := len(forecast.Values)

	if len(r) == 0 {
		r = append(r, dataframe.Range{Start: &[]int{-nPred}[0]})
	}

	// Check if the range of testSeries matches the range of forecast
	nTest, err := r[0].NRows(len(testSeries.Values))
	if err != nil {
		return 0.0, 0, err
	}

	if nTest != nPred {
		return 0.0, 0, ErrMismatchLen
	}

	// Calculate SSE

	start, end, err := r[0].Limits(len(testSeries.Values))
	if err != nil {
		return 0.0, 0, err
	}

	var (
		n   int
		sum float64
	)

	// Loop through testSeries starting at start.
	// Loop through forecast starting at 0.
	for i, j := start, 0; i < end+1; i, j = i+1, j+1 {

		if err := ctx.Err(); err != nil {
			return 0.0, 0, err
		}

		actual := testSeries.Values[i]
		predicted := forecast.Values[j]

		if isInvalidFloat64(actual) || isInvalidFloat64(predicted) {
			if opts != nil && opts.SkipInvalids {
				continue
			} else {
				return 0.0, 0, ErrIndeterminate
			}
		}

		e := actual - predicted

		sum = sum + e*e
		n = n + 1
	}

	return sum, n, nil
}

// RootMeanSquaredError represents the root mean squared error.
//
// See: https://otexts.com/fpp2/accuracy.html
func RootMeanSquaredError(ctx context.Context, testSeries, forecast *dataframe.SeriesFloat64, opts *ErrorOptions, r ...dataframe.Range) (float64, int, error) {

	if opts == nil || (opts != nil && !opts.DontLock) {
		// Lock both series
		testSeries.Lock()
		forecast.Lock()
		defer testSeries.Unlock()
		defer forecast.Unlock()
	}

	if opts != nil {
		if !opts.DontLock {
			opts2 := *opts
			opts2.DontLock = true
			opts = &opts2
		}
	} else {
		opts = &ErrorOptions{DontLock: true}
	}

	sse, n, err := SumOfSquaredErrors(ctx, testSeries, forecast, opts, r...)
	if err != nil {
		return 0.0, 0, err
	}

	if n == 0 {
		return 0.0, 0, ErrIndeterminate
	}

	return math.Sqrt(sse / float64(n)), n, nil
}

// MeanAbsolutePercentageError represents the mean absolute percentage error.
//
// See: https://otexts.com/fpp2/accuracy.html
func MeanAbsolutePercentageError(ctx context.Context, testSeries, forecast *dataframe.SeriesFloat64, opts *ErrorOptions, r ...dataframe.Range) (float64, int, error) {

	if opts == nil || (opts != nil && !opts.DontLock) {
		// Lock both series
		testSeries.Lock()
		forecast.Lock()
		defer testSeries.Unlock()
		defer forecast.Unlock()
	}

	nPred := len(forecast.Values)

	if len(r) == 0 {
		r = append(r, dataframe.Range{Start: &[]int{-nPred}[0]})
	}

	// Check if the range of testSeries matches the range of forecast
	nTest, err := r[0].NRows(len(testSeries.Values))
	if err != nil {
		return 0.0, 0, err
	}

	if nTest != nPred {
		return 0.0, 0, errors.New("mismatch length")
	}

	// Calculate MAPE

	start, end, err := r[0].Limits(len(testSeries.Values))
	if err != nil {
		return 0.0, 0, err
	}

	var (
		n   int
		sum float64
	)

	// Loop through testSeries starting at start.
	// Loop through forecast starting at 0.
	for i, j := start, 0; i < end+1; i, j = i+1, j+1 {

		if err := ctx.Err(); err != nil {
			return 0.0, 0, err
		}

		actual := testSeries.Values[i]
		predicted := forecast.Values[j]

		if isInvalidFloat64(actual) || isInvalidFloat64(predicted) || actual == 0 {
			if opts != nil && opts.SkipInvalids {
				continue
			} else {
				return 0.0, 0, ErrIndeterminate
			}
		}

		e := actual - predicted

		sum = sum + math.Abs(100*e/actual)
		n = n + 1
	}

	if n == 0 {
		return 0.0, 0, ErrIndeterminate
	}

	return sum / float64(n), n, nil
}
