package forecast

import (
	"context"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	pd "github.com/rocketlaunchr/dataframe-go/pandas"
)

// DataType is a Enum used to
// specify data type selection from Sesmodel
type DataType int

const (
	// MainData type specifies selection of the original data from Model
	MainData DataType = 0
	// TrainData type specifies selection of trainData from Model
	TrainData DataType = 1
	// TestData type specifies selection of testData from Model
	TestData DataType = 2
)

// Model is an interface to group trained models of Different
// Algorithms in the Forecast package under similar generic standard
type Model interface {
	// Fit Method performs the splitting and training of the Model Interface based on the Forecast algorithm Implemented.
	// It returns a trained Model ready to carry out future forecasts.
	Fit(context.Context, *dataframe.Range, interface{}) (Model, error)

	// Predict method is used to run future predictions for the Model algorithm
	// It returns an interface{} result that is either dataframe.SeriesFloat64 or dataframe.Dataframe format
	Predict(context.Context, int) (interface{}, error)

	// Summary method is used to Print out Data Summary
	// From the Trained Model
	Summary()

	// Describe method
	Describe(context.Context, DataType, ...pd.DescribeOptions)
}
