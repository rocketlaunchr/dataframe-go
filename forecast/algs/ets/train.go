// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package ets

import (
	"context"
)

type trainingState struct {
	initialLevel   float64
	originValue    float64
	smoothingLevel float64
}

func (es *ExponentialSmoothing) trainSeries(ctx context.Context, start, end int) error {

	var (
		α       float64 = es.cfg.Alpha
		st      float64
		Yorigin float64
	)

	// Training smoothing Level
	for i := start; i < end+1; i++ {

		if err := ctx.Err(); err != nil {
			return err
		}

		xt := es.sf.Values[i]

		if i == start {
			st = xt
			es.tstate.initialLevel = xt
		} else if i == end { // Setting the last value in traindata as Yorigin value for bootstrapping
			Yorigin = xt
			es.tstate.originValue = Yorigin
		} else {
			st = α*xt + (1-α)*st
		}
	}
	es.tstate.smoothingLevel = st

	return nil
}
