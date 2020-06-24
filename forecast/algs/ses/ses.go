// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package ses

import (
	"context"
	"errors"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/forecast"
)

// ExponentialSmoothingConfig is used to configure the SES algorithm.
// SES models the error, trend and seasonal elements of the data with exponential smoothing.
//
// NOTE: SES algorithm does not tolerate nil values. You may need to use the interpolation subpackage.
type ExponentialSmoothingConfig struct {

	// Alpha must be between 0 and 1. The closer Alpha is to 1, the more the algorithm
	// prioritises recent values over past values.
	Alpha float64

	// ConfidenceLevels are values between 0 and 1 (exclusive) that return the associated
	// confidence intervals for each forecasted value.
	//
	// See: https://otexts.com/fpp2/prediction-intervals.html
	ConfidenceLevels []float64
}

// Validate checks if the config is valid.
func (cfg *ExponentialSmoothingConfig) Validate() error {
	if (cfg.Alpha < 0.0) || (cfg.Alpha > 1.0) {
		return errors.New("Alpha must be between [0,1]")
	}

	for _, c := range cfg.ConfidenceLevels {
		if c <= 0.0 || c >= 1.0 {
			return errors.New("ConfidenceLevel value must be between (0,1)")
		}
	}

	return nil
}

// SimpleExpSmoothing represents the SES algorithm for time-series forecasting.
// It uses the bootstrapping method found here: https://www.itl.nist.gov/div898/handbook/pmc/section4/pmc432.htm
type SimpleExpSmoothing struct {
	tstate trainingState
	cfg    ExponentialSmoothingConfig
	tRange dataframe.Range // training range
	sf     *dataframe.SeriesFloat64
}

// NewExponentialSmoothing creates a new SimpleExpSmoothing object.
func NewExponentialSmoothing() *SimpleExpSmoothing {
	return &SimpleExpSmoothing{}
}

// Configure sets the various parameters for the SES algorithm.
// config must be a ExponentialSmoothingConfig.
func (se *SimpleExpSmoothing) Configure(config interface{}) error {

	cfg := config.(ExponentialSmoothingConfig)
	if err := cfg.Validate(); err != nil {
		return err
	}

	se.cfg = cfg
	return nil
}

// Load loads historical data.
// r is used to limit which rows of sf are loaded. Prediction will always begin
// from the row after that defined by r. r can be thought of as defining a "training set".
//
// NOTE: SES algorithm does not tolerate nil values. You may need to use the interpolation subpackage.
func (se *SimpleExpSmoothing) Load(ctx context.Context, sf *dataframe.SeriesFloat64, r *dataframe.Range) error {

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

	// at least 3 observations required for SES.
	if e-s < 2 {
		return forecast.ErrInsufficientDataPoints
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

	se.tRange = *r
	se.sf = sf
	se.tstate = trainingState{}

	err = se.trainSeries(ctx, uint(s), uint(e))
	if err != nil {
		se.tRange = dataframe.Range{}
		se.sf = nil
		return err
	}

	return nil
}
