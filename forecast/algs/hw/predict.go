// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package hw

import (
	"context"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/forecast"
)

// Predict forecasts the next n values for the loaded data.
func (hw *HoltWinters) Predict(ctx context.Context, n uint) (*dataframe.SeriesFloat64, []forecast.Confidence, error) {

	name := hw.sf.Name(dataframe.DontLock)
	nsf := dataframe.NewSeriesFloat64(name, &dataframe.SeriesInit{Capacity: int(n)})

	if n <= 0 {
		if len(hw.cfg.ConfidenceLevels) == 0 {
			return nsf, nil, nil
		}
		return nsf, []forecast.Confidence{}, nil
	}

	cnfdnce := []forecast.Confidence{}

	var (
		st        float64   = hw.tstate.smoothingLevel
		seasonals []float64 = hw.tstate.seasonalComps
		trnd      float64   = hw.tstate.trendLevel
		period    int       = hw.cfg.Period
	)

	for i := uint(0); i < n; i++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		m := int(i + 1)

		var fval float64
		if hw.cfg.SeasonalMethod == Multiply {
			fval = (st + float64(m)*trnd) * seasonals[(m-1)%period]
		} else {
			fval = (st + float64(m)*trnd) + seasonals[(m-1)%period]
		}
		nsf.Append(fval, dataframe.DontLock)

		cis := map[float64]forecast.ConfidenceInterval{}
		for _, level := range hw.cfg.ConfidenceLevels {
			cis[level] = forecast.DriftConfidenceInterval(fval, level, hw.tstate.rmse, hw.tstate.T, n)
		}
		cnfdnce = append(cnfdnce, cis)
	}

	if len(hw.cfg.ConfidenceLevels) == 0 {
		return nsf, nil, nil
	} else {
		return nsf, cnfdnce, nil
	}
}
