// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package ses

import (
	"context"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// Predict forecasts the next n values for the loaded data.
func (se *SimpleExpSmoothing) Predict(ctx context.Context, n uint) (*dataframe.SeriesFloat64, error) {

	name := se.sf.Name(dataframe.DontLock)
	nsf := dataframe.NewSeriesFloat64(name, &dataframe.SeriesInit{Capacity: int(n)})

	if n <= 0 {
		return nsf, nil
	}

	var (
		α       float64 = se.cfg.Alpha
		st      float64 = se.tstate.smoothingLevel
		Yorigin float64 = se.tstate.originValue
	)

	for i := uint(0); i < n; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		pred := α*Yorigin + (1-α)*st

		nsf.Values = append(nsf.Values, pred)

		// TODO: calculate Confidence interval
		//

	}

	return nsf, nil
}
