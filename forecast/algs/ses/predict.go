// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package ses

import (
	"context"
	"math"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/forecast"
)

// Predict forecasts the next n values for the loaded data.
func (se *SimpleExpSmoothing) Predict(ctx context.Context, n uint) (*dataframe.SeriesFloat64, []forecast.Confidence, error) {

	name := se.sf.Name(dataframe.DontLock)
	nsf := dataframe.NewSeriesFloat64(name, &dataframe.SeriesInit{Capacity: int(n)})

	if n <= 0 {
		return nsf, nil, nil
	}

	var (
		α       float64 = se.cfg.Alpha
		st      float64 = se.tstate.smoothingLevel
		Yorigin float64 = se.tstate.originValue
		cnfdnce []forecast.Confidence
	)

	for i := uint(0); i < n; i++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		pred := α*Yorigin + (1-α)*st

		nsf.Values = append(nsf.Values, pred)

		// calculate Confidence interval
		cnfInt := map[float64]forecast.ConfidenceInterval{}

		for l, zVal := range se.tstate.zValues {
			// formular(Naive mthd): pred +/- (zVal*rmse) * sqrt(i+1)
			lowerInt := pred - (zVal*se.tstate.rmse)*math.Sqrt(float64(i)+1.0)
			upperInt := pred + (zVal*se.tstate.rmse)*math.Sqrt(float64(i)+1.0)
			cnfInt[l] = forecast.ConfidenceInterval{
				Upper: upperInt,
				Lower: lowerInt,
			}
		}
		cnfdnce = append(cnfdnce, cnfInt)
	}

	return nsf, cnfdnce, nil
}
