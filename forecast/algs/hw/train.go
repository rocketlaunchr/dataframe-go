// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package hw

import (
	"context"
	"math"

	"github.com/rocketlaunchr/dataframe-go/forecast"
)

type trainingState struct {
	initialSmooth        float64
	initialTrend         float64
	initialSeasonalComps []float64
	smoothingLevel       float64
	trendLevel           float64
	seasonalComps        []float64
	rmse                 float64
	zValues              map[float64]float64
}

func (hw *HoltWinters) trainSeries(ctx context.Context, start, end int) error {

	var (
		α, β, γ        float64 = hw.cfg.Alpha, hw.cfg.Beta, hw.cfg.Gamma
		period         int     = hw.cfg.Period
		trnd, prevTrnd float64 // trend
		st, prevSt     float64 // smooth
		mse            float64 // mean squared error
	)

	y := hw.sf.Values[start : end+1]

	seasonals := initialSeasonalComponents(y, period, hw.cfg.Seasonal)

	hw.tstate.initialSeasonalComps = initialSeasonalComponents(y, period, hw.cfg.Seasonal)

	trnd = initialTrend(y, period)
	hw.tstate.initialTrend = trnd

	count := 0

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
			if hw.cfg.Seasonal == MULTIPLY {
				// multiplicative method
				prevSt, st = st, α*(xt/seasonals[i%period])+(1-α)*(st+trnd)
				trnd = β*(st-prevSt) + (1-β)*trnd
				seasonals[i%period] = γ*(xt/st) + (1-γ)*seasonals[i%period]
			} else {
				// additive method
				prevSt, st = st, α*(xt-seasonals[i%period])+(1-α)*(st+trnd)
				prevTrnd, trnd = trnd, β*(st-prevSt)+(1-β)*trnd
				seasonals[i%period] = γ*(xt-prevSt-prevTrnd) + (1-γ)*seasonals[i%period]
			}

			mse += (xt - seasonals[i%period]) * (xt - seasonals[i%period])
			count++
		}

	}
	mse /= float64(count)

	// calculate ZValues from confidence levels
	zVals := make(map[float64]float64, len(hw.cfg.ConfidenceLevels))
	for _, l := range hw.cfg.ConfidenceLevels {
		zVals[l] = forecast.ConfidenceLevelToZ(l)
	}

	hw.tstate.rmse = math.Sqrt(mse)
	hw.tstate.zValues = zVals

	hw.tstate.smoothingLevel = st
	hw.tstate.trendLevel = trnd
	hw.tstate.seasonalComps = seasonals

	return nil
}
