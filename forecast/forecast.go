// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package forecast

import (
	"context"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// Forecast predicts the next n values of sdf using the forecasting algorithm alg.
// cfg is required to configure the parameters of the algorithm. r is used to select a subset of sdf to
// be the "training set". Values after r form the "validation set". evalFunc can be set to measure the
// quality of the predictions. sdf can be a SeriesFloat64 or a DataFrame. DataFrame input is not yet implemented.
//
// NOTE: You can find basic forecasting algorithms in forecast/algs subpackage.
func Forecast(ctx context.Context, sdf interface{}, r *dataframe.Range, alg ForecastingAlgorithm, cfg interface{}, n uint, evalFunc EvaluationFunc) (interface{}, float64, error) {

	switch sdf := sdf.(type) {
	case *dataframe.SeriesFloat64:

		err := alg.Configure(cfg)
		if err != nil {
			return nil, 0, err
		}

		err = alg.Load(ctx, sdf, r)
		if err != nil {
			return nil, 0, err
		}

		pred, err := alg.Predict(ctx, n)
		if err != nil {
			return nil, 0, err
		}

		var errVal float64
		if evalFunc != nil {
			errVal, err = alg.Evaluate(ctx, sdf, evalFunc)
			if err != nil {
				return nil, 0, err
			}
		}

		return pred, errVal, nil

	case *dataframe.DataFrame:
		panic("sdf as a DataFrame is not yet implemented")
	default:
		panic("sdf must be a Series or DataFrame")
	}

	panic("no reach")
}
