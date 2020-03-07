package forecast

import (
	"context"
	"errors"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func calculateError(ctx context.Context, actual, expected *dataframe.SeriesFloat64, errorType ErrorType, opts ...*ErrorOptions) (*ErrorMeasurement, error) {
		
	// Calculate error measurement between actual and expected dataSet
	var (
		errVal float64
		err       error
		errOpts *ErrorOptions
	)

	if len(opts) == 0 {
		opts = append(opts, &ErrorOptions{})
	}
	errOpts = opts[0]

	if errorType == MAE {
		errVal, _, err = MeanAbsoluteError(ctx, actual, expected, errOpts)
		if err != nil {
			return nil, err
		}
	} else if errorType == SSE {
		errVal, _, err = SumOfSquaredErrors(ctx, actual, expected, errOpts)
		if err != nil {
			return nil, err
		}
	} else if errorType == RMSE {
		errVal, _, err = RootMeanSquaredError(ctx, actual, expected, errOpts)
		if err != nil {
			return nil, err
		}
	} else if errorType == MAPE {
		errVal, _, err = MeanAbsolutePercentageError(ctx, actual, expected, errOpts)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("Unknown error type")
	}

	return &ErrorMeasurement{errType: errorType, value: errVal}, nil
}
