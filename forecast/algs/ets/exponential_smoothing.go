// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package ets

import (
	"context"
	"errors"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/forecast"
)

// ExponentialSmoothingConfig is used to configure the ETS algorithm.
// ETS models the error, trend and seasonal elements of the data with exponential smoothing.
//
// NOTE: ETS algorithm does not tolerate nil values. You may need to use the interpolation subpackage.
type ExponentialSmoothingConfig struct {

	// Alpha must be between 0 and 1. The closer Alpha is to 1, the more the algorithm
	// prioritises recent values over past values.
	Alpha float64
}

func (cfg *ExponentialSmoothingConfig) Validate() error {
	if (cfg.Alpha < 0.0) || (cfg.Alpha > 1.0) {
		return errors.New("Alpha must be between [0,1]")
	}

	return nil
}

// ExponentialSmoothing represents the ETS algorithm for time-series forecasting.
type ExponentialSmoothing struct {
	tstate trainingState
	cfg    ExponentialSmoothingConfig
	tRange dataframe.Range // training range
	sf     *dataframe.SeriesFloat64
}

func NewExponentialSmoothing() *ExponentialSmoothing {
	return &ExponentialSmoothing{}
}

// Configure sets the various parameters for the ETS algorithm.
// config must be a ExponentialSmoothingConfig.
func (es *ExponentialSmoothing) Configure(config interface{}) error {

	cfg := config.(ExponentialSmoothingConfig)
	if err := cfg.Validate(); err != nil {
		return err
	}

	es.cfg = cfg
	return nil
}

// Load loads historical data.
// r is used to limit which rows of sf are loaded. Prediction will always begin
// from the row after that defined by r. r can be thought of as defining a "training set".
//
// NOTE: ETS algorithm does not tolerate nil values. You may need to use the interpolation subpackage.
func (es *ExponentialSmoothing) Load(ctx context.Context, sf *dataframe.SeriesFloat64, r *dataframe.Range) error {

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

	es.tRange = *r
	es.sf = sf

	err = es.trainSeries(ctx, s, e)
	if err != nil {
		return err
	}

	return nil
}
