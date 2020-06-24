// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package ses

import (
	"context"
	"math"
)

type trainingState struct {
	finalSmoothed *float64 // stores the smoothed value of the final observation point
	yOrigin       float64
	rmse          float64
	T             uint // how many observed values used in the forcasting process
}

func (se *SimpleExpSmoothing) trainSeries(ctx context.Context, start, end uint) error {

	var α float64 = se.cfg.Alpha

	var mse float64

	// Step 1: Calculate Smoothed values for existing observations
	for i, j := start, 1; i < end+1; i, j = i+1, j+1 {
		if err := ctx.Err(); err != nil {
			return err
		}

		if j == 1 {
			// not applicable
		} else if j == 2 {
			se.tstate.finalSmoothed = &se.sf.Values[start]
		} else {
			St := α*se.sf.Values[i-1] + (1-α)**se.tstate.finalSmoothed
			se.tstate.finalSmoothed = &St

			err := se.sf.Values[i] - St // actual value - smoothened value
			mse = mse + err*err
		}
	}
	se.tstate.T = end - start + 1

	// Step 2: Store the y origin
	se.tstate.yOrigin = se.sf.Values[end]

	// Step 3: Calculate rmse
	se.tstate.rmse = math.Sqrt(mse / float64(end-start-1))

	return nil
}
