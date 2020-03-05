package forecast

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/bradfitz/iter"
	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/utils/utime"
)

type trainingState struct {
}

type tsGen struct {
	tsInterval   string
	tsIntReverse bool
	lastTsVal    time.Time
}

// EtsModel is a struct that holds necessary
// computed values for a forecasting result
type EtsModel struct {
	data           []float64
	trainData      []float64
	testData       []float64
	initialLevel   float64
	originValue    float64
	smoothingLevel float64
	alpha          float64
	inputIsDf      bool
	ts             tsGen
	training       trainingState
}

type ExponentialSmoothing struct {
	data []float
}

type HoltWinters struct {
}

func NewEtsModel() *EtsModel {

	model := &EtsModel{
		alpha:          0.0,
		data:           []float64{},
		trainData:      []float64{},
		testData:       []float64{},
		initialLevel:   0.0,
		smoothingLevel: 0.0,
		inputIsDf:      false,
	}

	return model
}

// Configure sets the various parameters for Ets Algorithm.
// config must be a ExponentialSmootheningConfig struct.
func (em *EtsModel) Configure(config interface{}) {

	cfg := config.(*ExponentialSmootheningConfig)

	if cfg, ok := config.(*ExponentialSmootheningConfig); ok {

		if (cfg.Alpha < 0.0) || (cfg.Alpha > 1.0) {
			panic("alpha must be between [0,1]")
		}

		em.alpha = cfg.Alpha

	} else {
		panic(fmt.Errorf("struct config parameter [%T] is not compartible with ets model Type: [*forecast.ExponentialSmootheningConfig]", cfg))
	}
}

// Load loads historical data. sdf can be a SeriesFloat64 or DataFrame.
func (em *EtsModel) Load(ctx context.Context, sdf interface{}, r *dataframe.Range) error {

	if r == nil {
		r = &dataframe.Range{}
	}

	sf := sdf.(*dataframe.SeriesFloat64)

	start, end, err := r.Limits(len(sf.Values))
	if err != nil {
		return err
	}

	em.datas = df.(*dataframe.SeriesFloat64).Values[start : end+1]

	return nil

	///////////////////

	ctx := context.Background()

	switch d := sdf.(type) {
	case *dataframe.SeriesFloat64:

		em.data = d.Values

	case *dataframe.DataFrame:

		err := d.Validate()

		err := em.loadDataFromDF(ctx, d)
		if err != nil {
			panic(err)
		}

	default:
		panic("unknown data format passed in. make sure you pass in a SeriesFloat64 or a forecast standard two(2) column dataframe.")
	}

	tr := &dataframe.Range{}
	if r != nil {
		tr = r
	}

	count := len(em.data)
	if count == 0 {
		panic(ErrNoRows)
	}

	start, end, err := tr.Limits(count)
	if err != nil {
		panic(err)
	}

	// Validation
	if end-start < 1 {
		panic("no values in selected series range")
	}

	trainData := em.data[start : end+1]
	em.trainData = trainData

	testData := em.data[end+1:]
	if len(testData) < 3 {
		panic("there should be a minimum of 3 data left as testing data")
	}
	em.testData = testData

	err = em.trainModel(ctx, start, end)
	if err != nil {
		panic(err)
	}
}

func (em *EtsModel) loadDataFromDF(ctx context.Context, d *dataframe.DataFrame) error {

	var (
		data      []float64
		isDf      bool
		tsInt     string
		tReverse  bool
		err       error
		tsName    string
		lastTsVal time.Time
	)

	isDf = true
	// validate that
	// - DataFrame has exactly two columns
	// - first column is SeriesTime
	// - second column is SeriesFloat64
	if len(d.Series) != 2 {

		return errors.New("dataframe passed in must have exactly two series/columns")
	}

	if d.Series[0].Type() == "time" {
		// get the current time interval/freq from the seriesTime
		if ts, ok := d.Series[0].(*dataframe.SeriesTime); ok {
			tsName = ts.Name(dataframe.DontLock)

			rowLen := ts.NRows(dataframe.DontLock)
			// store the last value in timeSeries column
			lastTsVal = ts.Value(rowLen-1, dataframe.DontLock).(time.Time)

			// guessing with only half the original time series row length
			// for efficiency
			half := rowLen / 2
			utimeOpts := utime.GuessTimeFreqOptions{
				R:        &dataframe.Range{End: &half},
				DontLock: true,
			}

			tsInt, tReverse, err = utime.GuessTimeFreq(ctx, ts, utimeOpts)
			if err != nil {
				return err
			}
		} else {
			return errors.New("column 0 not convertible to SeriesTime")
		}
	} else {
		return errors.New("first column/series must be a SeriesTime")
	}

	if d.Series[1].Type() == "float64" {
		val := d.Series[1].Copy()
		if v, ok := val.(*dataframe.SeriesFloat64); ok {
			data = v.Values
		} else {
			return errors.New("column 1 not convertible to SeriesFloat64")
		}
	} else {
		return errors.New("second column/series must be a SeriesFloat64")
	}

	em.data = data
	em.inputIsDf = isDf
	em.tsInterval = tsInt
	em.tsIntReverse = tReverse
	em.tsName = tsName
	em.lastTsVal = lastTsVal

	return nil
}

func (em *EtsModel) trainModel(ctx context.Context, start, end int) error {
	var (
		α, st, Yorigin float64
	)

	α = em.alpha

	// Training smoothing Level
	for i := start; i < end+1; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		xt := em.data[i]

		if i == start {
			st = xt
			em.initialLevel = xt

		} else if i == end { // Setting the last value in traindata as Yorigin value for bootstrapping
			Yorigin = em.data[i]
			em.originValue = Yorigin
		} else {
			st = α*xt + (1-α)*st
		}
	}
	em.smoothingLevel = st

	return nil
}

// Validate can be used by providing a validation set of data.
// It will then forecast the values from the end of the loaded data and then compare
// them with the validation set.
func (em *EtsModel) Validate(ctx context.Context, sdf interface{}, r *dataframe.Range, errorType ErrorType) (float64, error) {

	var (
		actualDataset   *dataframe.SeriesFloat64
		forecastDataset *dataframe.SeriesFloat64
		errVal          float64
	)

	tr := &dataframe.Range{}
	if r != nil {
		tr = r
	}

	switch d := sdf.(type) {
	case *dataframe.SeriesFloat64:

		val := d.Copy(*tr)
		if v, ok := val.(*dataframe.SeriesFloat64); ok {
			actualDataset = v
		} else {
			return math.NaN(), errors.New("series data is not SeriesFloat64")
		}

	case *dataframe.DataFrame:

		if val, ok := d.Series[1].(*dataframe.SeriesFloat64); ok {
			actualDataset = val.Copy(*tr).(*dataframe.SeriesFloat64)
		} else {
			return math.NaN(), errors.New("series data is not SeriesFloat64")
		}

	default:
		return math.NaN(), errors.New("unknown data format passed in. make sure you pass in a SeriesFloat64 or a forecast standard two(2) column dataframe")
	}

	m := len(actualDataset.Values)
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

	// Calculate error measurement between forecast and actual dataSet
	errOpts := &ErrorOptions{}

	if errorType == MAE {
		errVal, _, err = MeanAbsoluteError(ctx, actualDataset, forecastDataset, errOpts)
		if err != nil {
			return math.NaN(), err
		}
	} else if errorType == SSE {
		errVal, _, err = SumOfSquaredErrors(ctx, actualDataset, forecastDataset, errOpts)
		if err != nil {
			return math.NaN(), err
		}
	} else if errorType == RMSE {
		errVal, _, err = RootMeanSquaredError(ctx, actualDataset, forecastDataset, errOpts)
		if err != nil {
			return math.NaN(), err
		}
	} else if errorType == MAPE {
		errVal, _, err = MeanAbsolutePercentageError(ctx, actualDataset, forecastDataset, errOpts)
		if err != nil {
			return math.NaN(), err
		}
	} else {
		return math.NaN(), errors.New("Unknown error type")
	}

	return errVal, nil
}

// Predict forecasts the next n values for a Series or DataFrame.
// If a Series was provided to Load function, then a Series is retured.
// Alternatively a DataFrame is returned.
func (em *EtsModel) Predict(ctx context.Context, n int) (interface{}, error) {
	if n <= 0 {
		return nil, errors.New("m must be greater than 0")
	}

	forecast := make([]float64, n)
	α := em.alpha
	Yorigin := em.originValue
	st := em.smoothingLevel

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
		ts, err := utime.NewSeriesTime(ctx, em.tsName, em.tsInterval, em.lastTsVal, em.tsIntReverse, opts)
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
