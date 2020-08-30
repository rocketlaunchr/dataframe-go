// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

// Package hw implements the Holt-Winters forecasting algorithm.
package hw

import (
	"context"
	"errors"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/forecast"
)

// Method specifies if the model type is additive or multiplicative.
type Method int

const (
	// Additive sets the model type to additive.
	Additive Method = 0

	// Multiplicative sets the model type to multiplicative.
	Multiplicative Method = 1
)

// HoltWintersConfig is used to configure the Holt-Winters algorithm.
//
// NOTE: Holt-Winters algorithm does not tolerate nil values. You may need to use the interpolation subpackage.
//
// See: https://otexts.com/fpp2/holt-winters.html
type HoltWintersConfig struct {

	// Alpha must be between 0 and 1. The closer Alpha is to 1, the more the algorithm
	// prioritizes recent values over past values.
	Alpha float64

	// Beta must be between 0 and 1. The closer Beta is to 1, the trend component will prioritize
	// recent values over past values.
	Beta float64

	// Gamma must be between 0 and 1. The closer Gamma is to 1, the more recent seasonal pattern is prioritized.
	Gamma float64

	// Period can be understood as follows:
	//
	// A season is a fixed length of time that contains the full repetition.
	// You might think your data repeats daily (there’s a peak at 2pm every day), but if the weekend has different behavior (there’s no peak at 2pm on Sunday) then your season is really a week, not a day.
	// Within the season, there are periods, which is the granularity of prediction.
	// If you want to model a value for every hour of every day within a week, your season is 168 hours long and your period is 1 hour.
	//
	// Therefore, a complete season's data consists of L periods. In the above example, L is 168. L is what you set for the Period config value.
	Period uint

	// SeasonalMethod sets whether the model is additive or multiplicative.
	// The default is additive.
	SeasonalMethod Method

	// ConfidenceLevels are values between 0 and 1 (exclusive) that return the associated
	// confidence intervals for each forecasted value.
	//
	// See: https://otexts.com/fpp2/prediction-intervals.html
	ConfidenceLevels []float64
}

// Validate checks if the config is valid.
func (cfg *HoltWintersConfig) Validate() error {
	if (cfg.Alpha < 0.0) || (cfg.Alpha > 1.0) {
		return errors.New("Alpha must be between [0,1]")
	}
	if (cfg.Beta < 0.0) || (cfg.Beta > 1.0) {
		return errors.New("Beta must be between [0,1]")
	}
	if (cfg.Gamma < 0.0) || (cfg.Gamma > 1.0) {
		return errors.New("Gamma must be between [0,1]")
	}
	if cfg.Period <= 2 {
		return errors.New("Period must be at least a length of 2")
	}

	for _, c := range cfg.ConfidenceLevels {
		if c <= 0.0 || c >= 1.0 {
			return errors.New("ConfidenceLevel value must be between (0,1)")
		}
	}

	return nil
}

// HoltWinters represents the Holt-Winters algorithm for time-series forecasting.
//
// See: https://otexts.com/fpp2/holt-winters.html
type HoltWinters struct {
	tstate trainingState
	cfg    HoltWintersConfig
	tRange dataframe.Range // training range
	sf     *dataframe.SeriesFloat64
}

// NewHoltWinters creates a new HoltWinters object.
func NewHoltWinters() *HoltWinters {
	return &HoltWinters{}
}

// Configure sets the various parameters for the HW algorithm.
// config must be a HoltWintersConfig.
func (hw *HoltWinters) Configure(config interface{}) error {

	cfg := config.(HoltWintersConfig)
	if err := cfg.Validate(); err != nil {
		return err
	}

	hw.cfg = cfg
	return nil
}

// Load loads historical data.
//
// sf is the series containing historical seasonal data.
// It must be at least a full season. For optimal results use at least two full seasons.
//
// r is used to limit which rows of sf are loaded. Prediction will always begin
// from the row after that defined by r. r can be thought of as defining a "training set".
//
// NOTE: Holt-Winters algorithm does not tolerate nil values. You may need to use the interpolation subpackage.
func (hw *HoltWinters) Load(ctx context.Context, sf *dataframe.SeriesFloat64, r *dataframe.Range) error {

	if r == nil {
		r = &dataframe.Range{}
	}

	tLength := sf.NRows(dataframe.DontLock)

	nrows, _ := r.NRows(tLength)
	if nrows == 0 {
		return forecast.ErrInsufficientDataPoints
	}

	s, e, err := r.Limits(tLength)
	if err != nil {
		return err
	}

	// Check if there are any nil values
	nils, err := sf.NilCount(dataframe.NilCountOptions{
		Ctx:          ctx,
		R:            r,
		StopAtOneNil: true,
		DontLock:     true,
	})
	if err != nil {
		return err
	}
	if nils > 0 {
		return forecast.ErrInsufficientDataPoints
	}

	// For data with m seasons per year, m+5 observations is theoretical minimum.
	// See: https://robjhyndman.com/papers/shortseasonal.pdf
	if e-s < int(hw.cfg.Period+5) {
		return forecast.ErrInsufficientDataPoints
	}

	hw.tRange = *r
	hw.sf = sf
	hw.tstate = trainingState{}

	err = hw.trainSeries(ctx, s, e)
	if err != nil {
		hw.tRange = dataframe.Range{}
		hw.sf = nil
		return err
	}

	return nil
}
