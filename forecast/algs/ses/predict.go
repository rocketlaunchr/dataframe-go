// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package ses

import (
	"context"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/forecast"
)

// Predict forecasts the next n values for the loaded data.
func (se *SimpleExpSmoothing) Predict(ctx context.Context, n uint) (*dataframe.SeriesFloat64, []forecast.Confidence, error) {

	iN := int(n)
	name := se.sf.Name(dataframe.DontLock)
	nsf := dataframe.NewSeriesFloat64(name, &dataframe.SeriesInit{Capacity: iN})

	if n <= 0 {
		if len(se.cfg.ConfidenceLevels) == 0 {
			return nsf, nil, nil
		}
		return nsf, []forecast.Confidence{}, nil
	}

	cnfdnce := []forecast.Confidence{}

	for i := 0; i < iN; i++ {
		StplusOne := se.cfg.Alpha*se.tstate.yOrigin + (1-se.cfg.Alpha)**se.tstate.finalSmoothed
		se.tstate.finalSmoothed = &StplusOne
		nsf.Append(StplusOne, dataframe.DontLock)

		cis := map[float64]forecast.ConfidenceInterval{}
		for _, level := range se.cfg.ConfidenceLevels {
			cis[level] = forecast.DriftConfidenceInterval(StplusOne, level, se.tstate.rmse, se.tstate.T, n)
		}
		cnfdnce = append(cnfdnce, cis)
	}

	if len(se.cfg.ConfidenceLevels) == 0 {
		return nsf, nil, nil
	} else {
		return nsf, cnfdnce, nil
	}
}
