// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package hw

import (
	"context"
)

type trainingState struct {
	initialSmooth        float64
	initialTrend         float64
	initialSeasonalComps []float64
	smoothingLevel       float64
	trendLevel           float64
	seasonalComps        []float64
}

func (hw *HoltWinters) trainSeries(ctx context.Context, start, end int) error {

	var (
		α, β, γ        float64 = hw.cfg.Alpha, hw.cfg.Beta, hw.cfg.Gamma
		period         int     = hw.cfg.Period
		trnd, prevTrnd float64 // trend
		st, prevSt     float64 // smooth
	)

	y := hw.sf.Values[start : end+1]

	seasonals := initialSeasonalComponents(y, period, hw.cfg.TsType)

	hw.tstate.initialSeasonalComps = initialSeasonalComponents(y, period, hw.cfg.TsType)

	trnd = initialTrend(y, period)
	hw.tstate.initialTrend = trnd

	// Training smoothing Level
	for i := start; i < end+1; i++ {

		if err := ctx.Err(); err != nil {
			return err
		}

		xt := y[i]

		if i == start { // Set initial smooth
			st = xt

			hw.tstate.initialSmooth = xt

		} else {
			if hw.cfg.TsType == MULTIPLY {
				// multiplicative method
				prevSt, st = st, α*(xt/seasonals[i%period])+(1-α)*(st+trnd)
				prevTrnd, trnd = trnd, β*(st-prevSt)+(1-β)*trnd
				seasonals[i%period] = γ*(xt/(prevSt+prevTrnd)) + (1-γ)*seasonals[i%period]
			} else {
				// additive method
				prevSt, st = st, α*(xt-seasonals[i%period])+(1-α)*(st+trnd)
				prevTrnd, trnd = trnd, β*(st-prevSt)+(1-β)*trnd
				seasonals[i%period] = γ*(xt-prevSt-prevTrnd) + (1-γ)*seasonals[i%period]
			}

		}

	}

	hw.tstate.smoothingLevel = st
	hw.tstate.trendLevel = trnd
	hw.tstate.seasonalComps = seasonals

	return nil
}
