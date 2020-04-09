// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package hw

import (
	"context"

	"github.com/bradfitz/iter"
	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// Predict forecasts the next n values for the loaded data.
func (hw *HoltWinters) Predict(ctx context.Context, n uint) (*dataframe.SeriesFloat64, error) {

	name := hw.sf.Name(dataframe.DontLock)
	nsf := dataframe.NewSeriesFloat64(name, &dataframe.SeriesInit{Capacity: int(n)})

	var (
		st        float64   = hw.tstate.smoothingLevel
		seasonals []float64 = hw.tstate.seasonalComps
		trnd      float64   = hw.tstate.trendLevel
		period    int       = hw.cfg.Period
	)

	forecast := make([]float64, n)

	m := 1
	pos := 0
	for range iter.N(n) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// multiplicative Method
		// forecast = append(forecast, (st + float64(m)*trnd) * seasonals[(m-1) % period])

		// additive method
		forecast[pos] = (st + float64(m)*trnd) + seasonals[(m-1)%period]

		m++
		pos++
	}

	nsf.Values = forecast

	return nsf, nil
}
