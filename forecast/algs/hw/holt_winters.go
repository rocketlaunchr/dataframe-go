// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package hw

import (
	"context"
	"errors"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/forecast"
)

// TimeSeriesType is used to set the time series type
type TimeSeriesType int

const (
	// ADD specifies additive method
	ADD TimeSeriesType = 0
	// MULTIPLY specifies additive method
	MULTIPLY TimeSeriesType = 1
)

// HoltWintersConfig is used to configure the HW algorithm.
// HW models the error, trend and seasonal elements of the data with holt winters.
//
// NOTE: HW algorithm does not tolerate nil values. You may need to use the interpolation subpackage.
type HoltWintersConfig struct {

	// Alpha, Beta and Gamma must be between 0 and 1.
	Alpha, Beta, Gamma float64

	// Period  is the length of the data season
	// It must be at least 2
	Period int

	// Seasonal is optional parameter used to specify the seasonality type
	// Additive [Add] or Multiplicative [Multiply]
	// Default method used is Add.
	Seasonal TimeSeriesType

	// ConfidenceLevels are values between 0 and 1 (exclusive) that return the associated
	// confidence intervals for each forecasted value.
	//
	// See: https://otexts.com/fpp2/prediction-intervals.html
	ConfidenceLevels []float64
}

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
	if cfg.Period < 2 {
		return errors.New("Period must be at least a length of 2")
	}

	for _, c := range cfg.ConfidenceLevels {
		if c < 0.0 || c > 1.0 {
			return errors.New("ConfidenceLevel value must be between [0,1]")
		}
	}

	return nil
}

// HoltWinters represents the HW algorithm for time-series forecasting.
type HoltWinters struct {
	tstate trainingState
	cfg    HoltWintersConfig
	tRange dataframe.Range // training range
	sf     *dataframe.SeriesFloat64
}

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
// sf is the series containing Historical Seasonal data.
// it must be at least a full season,
// for optimal results use at least two full seasons.
//
// r is used to limit which rows of sf are loaded. Prediction will always begin
// from the row after that defined by r. r can be thought of as defining a "training set".
//
// NOTE: HW algorithm does not tolerate nil values. You may need to use the interpolation subpackage.
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

	// How many minimum rows should we accept
	// TODO: return ErrInsufficientDataPoints
	// if e-s < 0 {
	// 	return ErrNoRows
	// }

	hw.tRange = *r
	hw.sf = sf

	err = hw.trainSeries(ctx, s, e)
	if err != nil {
		return err
	}

	return nil
}
