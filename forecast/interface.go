package forecast

import (
	dataframe "github.com/rocketlaunchr/dataframe-go"
)

type Algorithm interface {

	// Load loads historical data. sdf can be a SeriesFloat64 or DataFrame.
	Load(sdf interface{}, r *dataframe.Range)

	// Forecast forecasts the next n values for a Series or DataFrame.
	// If a Series was provided to Load function, then a Series is retured.
	// Alternatively a DataFrame is returend.
	Forecast(n int) interface{}

	// Configure sets the various parameters for the Algorithm.
	// config must be a struct that the particular Algorithm understands.
	Configure(config interface{})

	// Validate can be used by providing a validation set of data.
	// It will then forecast the values from the end of the loaded data and then compare
	// them with the validation set.
	Validate(sdf interface{}, r *dataframe.Range, errorType ErrorType) float64
}

// ExponentialSmootheningConfig is used to configure the ETS algorith.
type ExponentialSmootheningConfig struct {
	Alpha float64
}
