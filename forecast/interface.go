package forecast

import (
	"context"
	"errors"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// Algorithm interface sets generic standards of similar methods
// implemented by the Forecast Algorithms
type Algorithm interface {

	// Load loads historical data. sdf can be a SeriesFloat64 or DataFrame.
	Load(ctx context.Context, sdf interface{}, r *dataframe.Range) error

	// Predict forecasts the next n values for a Series or DataFrame.
	// If a Series was provided to Load function, then a Series is retured.
	// Alternatively a DataFrame is returned.
	Predict(ctx context.Context, n int) (interface{}, error)

	// Configure sets the various parameters for the Algorithm.
	// config must be a struct that the particular Algorithm understands.
	Configure(config interface{})

	// Validate can be used by providing a validation set of data.
	// It will then forecast the values from the end of the loaded data and then compare
	// them with the validation set.
	Validate(ctx context.Context, sdf interface{}, r *dataframe.Range, errorType ErrorType) (float64, error)
}

// ExponentialSmoothingConfig is used to configure the ETS algorithm.
type ExponentialSmoothingConfig struct {
	Alpha          float64
	ErrMeasurement *ErrorMeasurement
}

func (cfg *ExponentialSmoothingConfig) Validate() error {
	if (cfg.Alpha < 0.0) || (cfg.Alpha > 1.0) {
		return errors.New("alpha must be between [0,1]")
	}

	return nil
}

// HoltWintersConfig is used to configure the HW algorithm.
type HoltWintersConfig struct {
	Alpha          float64
	Beta           float64
	Gamma          float64
	Period         int
	ErrMeasurement *ErrorMeasurement
}

// func (cfg *HoltWintersConfig) Validate() error {

// }

type tsGen struct {
	tsName       string
	tsInterval   string
	tsIntReverse bool
	lastTsVal    time.Time
}
