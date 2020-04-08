// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package evaluation

import (
	"context"
	"math"

	"github.com/rocketlaunchr/dataframe-go/forecast"
)

// RootMeanSquaredError represents the root mean squared error.
//
// See: https://otexts.com/fpp2/accuracy.html
var RootMeanSquaredError = func(ctx context.Context, validationSet, forecastSet []float64, opts *forecast.EvaluationFuncOptions) (float64, int, error) {

	// Check if validationSet and forecastSet are the same size
	if len(validationSet) != len(forecastSet) {
		return 0, 0, forecast.ErrMismatchLen
	}

	if len(validationSet) == 0 {
		return 0.0, 0, forecast.ErrIndeterminate
	}

	sse, n, err := SumOfSquaredErrors(ctx, validationSet, forecastSet, opts)
	if err != nil {
		return 0.0, 0, err
	}

	return math.Sqrt(sse / float64(n)), n, nil
}
