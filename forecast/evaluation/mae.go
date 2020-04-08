// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package evaluation

import (
	"context"
	"math"

	"github.com/rocketlaunchr/dataframe-go/forecast"
)

// MeanAbsoluteError represents the mean absolute error.
//
// See: https://otexts.com/fpp2/accuracy.html
var MeanAbsoluteError = func(ctx context.Context, validationSet, forecastSet []float64, opts *forecast.EvaluationFuncOptions) (float64, int, error) {

	// Check if validationSet and forecastSet are the same size
	if len(validationSet) != len(forecastSet) {
		return 0, 0, forecast.ErrMismatchLen
	}

	var (
		n   int
		sum float64
	)

	for i := 0; i < len(validationSet); i++ {

		if err := ctx.Err(); err != nil {
			return 0.0, 0, err
		}

		actual := validationSet[i]
		predicted := forecastSet[i]

		if isInvalidFloat64(actual) || isInvalidFloat64(predicted) {
			if opts != nil && opts.SkipInvalids {
				continue
			} else {
				return 0.0, 0, forecast.ErrIndeterminate
			}
		}

		e := actual - predicted

		sum = sum + math.Abs(e)
		n = n + 1
	}

	if n == 0 {
		return 0.0, 0, forecast.ErrIndeterminate
	}

	return sum / float64(n), n, nil
}
