package forecast

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/bradfitz/iter"
	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/utils/utime"
)

type esTrainingState struct {
	initialLevel   float64
	originValue    float64
	smoothingLevel float64
}

// ExponentialSmoothing is a struct that holds necessary
// computed values for a forecasting result
type ExponentialSmoothing struct {
	data      []float64
	trainData []float64
	testData  []float64
	alpha     float64
	inputIsDf bool
	training  esTrainingState
	ts        tsGen
}

func NewExponentialSmoothing() *ExponentialSmoothing {

	model := &ExponentialSmoothing{
		data:      []float64{},
		trainData: []float64{},
		testData:  []float64{},
		inputIsDf: false,
	}

	return model
}

// Configure sets the various parameters for Ets Algorithm.
// config must be a ExponentialSmoothingConfig struct.
func (em *ExponentialSmoothing) Configure(config interface{}) {

	cfg := config.(*ExponentialSmoothingConfig)
	if err := cfg.Validate(); err != nil {
		panic(err)
	}

	em.alpha = cfg.Alpha

}

// Load loads historical data. sdf can be a SeriesFloat64 or DataFrame.
func (em *ExponentialSmoothing) Load(ctx context.Context, sdf interface{}, r *dataframe.Range) error {

	switch d := sdf.(type) {
	case *dataframe.SeriesFloat64:

		em.data = d.Values

	case *dataframe.DataFrame:

		// err := d.Validate()

		err := loadDataFromDF(ctx, em, d)
		if err != nil {
			panic(err)
		}

	default:
		panic("unknown data format passed in. make sure you pass in a SeriesFloat64 or a forecast standard two(2) column dataframe.")
	}

	if r == nil {
		r = &dataframe.Range{}
	}

	count := len(em.data)
	if count == 0 {
		panic(ErrNoRows)
	}

	start, end, err := r.Limits(count)
	if err != nil {
		panic(err)
	}

	err = splitModelData(em, start, end)
	if err != nil {
		panic(err)
	}

	err = trainModel(ctx, em, start, end)
	if err != nil {
		panic(err)
	}

	return nil
}

// Validate can be used by providing a validation set of data.
// It will then forecast the values from the end of the loaded data and then compare
// them with the validation set.
func (em *ExponentialSmoothing) Validate(ctx context.Context, sdf interface{}, r *dataframe.Range, errorType ErrorType) (float64, error) {

	var (
		expectedDataset *dataframe.SeriesFloat64
		forecastDataset *dataframe.SeriesFloat64
	)

	if r == nil {
		r = &dataframe.Range{}
	}

	switch d := sdf.(type) {
	case *dataframe.SeriesFloat64:

		val := d.Copy(*r)
		expectedDataset = val.(*dataframe.SeriesFloat64)

	case *dataframe.DataFrame:

		if val, ok := d.Series[1].(*dataframe.SeriesFloat64); ok {
			expectedDataset = val.Copy(*r).(*dataframe.SeriesFloat64)
		} else {
			return math.NaN(), errors.New("series data is not SeriesFloat64")
		}

	default:
		return math.NaN(), errors.New("unknown data format passed in. make sure you pass in a SeriesFloat64 or a forecast standard two(2) column dataframe")
	}

	m := len(expectedDataset.Values)
	forecast, err := em.Predict(ctx, m)
	if err != nil {
		return math.NaN(), err
	}

	switch f := forecast.(type) {
	case *dataframe.SeriesFloat64:
		forecastDataset = f
	case *dataframe.DataFrame:
		forecastDataset = f.Series[1].(*dataframe.SeriesFloat64)
	}

	errResult, err := calculateError(ctx, forecastDataset, expectedDataset, errorType)
	if err != nil {
		return math.NaN(), err
	}

	return errResult.Value(), nil
}

// Predict forecasts the next n values for a Series or DataFrame.
// If a Series was provided to Load function, then a Series is retured.
// Alternatively a DataFrame is returned.
func (em *ExponentialSmoothing) Predict(ctx context.Context, n int) (interface{}, error) {
	if n <= 0 {
		return nil, errors.New("m must be greater than 0")
	}

	forecast := make([]float64, n)
	α := em.alpha
	Yorigin := em.training.originValue
	st := em.training.smoothingLevel

	// Now calculate forecast
	pos := 0
	for range iter.N(n) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		st = α*Yorigin + (1-α)*st
		forecast[pos] = st

		pos++
	}

	fdf := dataframe.NewSeriesFloat64("Prediction", nil)
	fdf.Values = forecast

	if em.inputIsDf {

		size := n + 1

		// generate SeriesTime to start and continue from where it stopped in data input
		opts := utime.NewSeriesTimeOptions{
			Size: &size,
		}
		ts, err := utime.NewSeriesTime(ctx, em.ts.tsName, em.ts.tsInterval, em.ts.lastTsVal, em.ts.tsIntReverse, opts)
		if err != nil {
			panic(fmt.Errorf("error encountered while generating time interval prediction: %v", err))
		}

		// trying to exclude the first starting time
		nTs := ts.Copy(dataframe.Range{Start: &[]int{1}[0]})

		// combine fdf and generated time series into a dataframe and return
		return dataframe.NewDataFrame(nTs, fdf), nil
	}

	return fdf, nil
}
