// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package hw

import (
	"context"
	"math"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/forecast"
)

// Predict forecasts the next n values for the loaded data.
func (hw *HoltWinters) Predict(ctx context.Context, n uint) (*dataframe.SeriesFloat64, []forecast.Confidence, error) {

	name := hw.sf.Name(dataframe.DontLock)
	nsf := dataframe.NewSeriesFloat64(name, &dataframe.SeriesInit{Capacity: int(n)})

	var (
		st        float64   = hw.tstate.smoothingLevel
		seasonals []float64 = hw.tstate.seasonalComps
		trnd      float64   = hw.tstate.trendLevel
		period    int       = hw.cfg.Period
		cnfdnce   []forecast.Confidence
	)

	fcast := make([]float64, n)

	m := 1
	pos := 0
	for i := uint(0); i < n; i++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		if hw.cfg.Seasonal == MULTIPLY {
			// multiplicative Method
			fcast[pos] = (st + float64(m)*trnd) * seasonals[(m-1)%period]
		} else {
			// additive method
			fcast[pos] = (st + float64(m)*trnd) + seasonals[(m-1)%period]
		}

		// calculate Confidence interval
		cnfInt := map[float64]forecast.ConfidenceInterval{}

		for l, zVal := range hw.tstate.zValues {
			// formular(Naive mthd): pred +/- (zVal*rmse) * sqrt(i+1)
			lowerInt := fcast[pos] - (zVal*hw.tstate.rmse)*math.Sqrt(float64(i)+1.0)
			upperInt := fcast[pos] + (zVal*hw.tstate.rmse)*math.Sqrt(float64(i)+1.0)
			cnfInt[l] = forecast.ConfidenceInterval{
				Upper: upperInt,
				Lower: lowerInt,
			}
		}
		cnfdnce = append(cnfdnce, cnfInt)

		m++
		pos++
	}

	nsf.Values = fcast

	return nsf, cnfdnce, nil
}
