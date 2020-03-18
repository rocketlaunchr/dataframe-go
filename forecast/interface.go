package forecast

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
	Alpha   float64
	TsCol   interface{}
	DataCol interface{}
}

func (cfg *ExponentialSmoothingConfig) Validate() error {
	if (cfg.Alpha < 0.0) || (cfg.Alpha > 1.0) {
		return errors.New("alpha must be between [0,1]")
	}

	dColTyp := reflect.TypeOf(cfg.DataCol)
	if dColTyp != reflect.TypeOf(int(1)) && dColTyp != reflect.TypeOf("s") && dColTyp != nil {
		return fmt.Errorf("datacol must be an int or a string input not [%T]", cfg.DataCol)
	}

	tsColTyp := reflect.TypeOf(cfg.TsCol)
	if tsColTyp != reflect.TypeOf(int(1)) && tsColTyp != reflect.TypeOf("s") && tsColTyp != nil {
		return fmt.Errorf("tscol must be an int or a string input not [%T]", cfg.TsCol)
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
	timeCol      interface{}
	dataCol      interface{}
	tsName       string
	tsInterval   string
	tsIntReverse bool
	lastTsVal    time.Time
}
