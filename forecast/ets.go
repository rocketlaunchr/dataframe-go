package forecast

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bradfitz/iter"
	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/utils/utime"
)

// EtsModel is a struct that holds necessary
// computed values for a forecasting result
type EtsModel struct {
	data           []float64
	trainData      []float64
	testData       []float64
	fcastData      *dataframe.SeriesFloat64
	initialLevel   float64
	originValue    float64
	smoothingLevel float64
	alpha          float64
	errorM         *ErrorMeasurement
	inputIsDf      bool
	tsInterval     string
	tsIntReverse   bool
	tsName         string
	lastTsVal      time.Time
}

func NewEtsModel() *EtsModel {
	model := &EtsModel{
		alpha:          0.0,
		data:           []float64{},
		trainData:      []float64{},
		testData:       []float64{},
		fcastData:      &dataframe.SeriesFloat64{},
		initialLevel:   0.0,
		smoothingLevel: 0.0,
		errorM:         &ErrorMeasurement{},
		inputIsDf:      false,
	}

	return model
}

// Configure sets the various parameters for Ets Algorithm.
// config must be a ExponentialSmootheningConfig struct.
func (em *EtsModel) Configure(config interface{}) {
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
func (em *EtsModel) Load(sdf interface{}, r *dataframe.Range) {
	ctx := context.Background()

	switch d := sdf.(type) {
	case *dataframe.SeriesFloat64:

		em.data = d.Values

	case *dataframe.DataFrame:

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

// Fit Method performs the splitting and training of the EtsModel based on the Exponential Smoothing algorithm.
// It returns a trained EtsModel ready to carry out future predictions.
// The argument α must be between [0,1].
func (em *EtsModel) Fit(ctx context.Context, tr *dataframe.Range, opts interface{}, et ...ErrorType) (*EtsModel, error) {

	var (
		α      float64
		r      *dataframe.Range
		errTyp ErrorType
	)

	if o, ok := opts.(ExponentialSmootheningConfig); ok {

		α = o.Alpha

	} else {
		return nil, errors.New("fit options passed is not compartible with ets model")
	}

	if tr != nil {
		r = tr
	}

	if len(et) > 0 {
		errTyp = et[0]
	}

	count := len(em.data)
	if count == 0 {
		return nil, ErrNoRows
	}

	start, end, err := r.Limits(count)
	if err != nil {
		return nil, err
	}

	// Validation
	if end-start < 1 {
		return nil, errors.New("no values in selected series range")
	}

	if (α < 0.0) || (α > 1.0) {
		return nil, errors.New("α must be between [0,1]")
	}

	em.alpha = α

	trainData := em.data[start : end+1]
	em.trainData = trainData

	testData := em.data[end+1:]
	if len(testData) < 3 {
		return nil, errors.New("There should be a minimum of 3 data left as testing data")
	}
	em.testData = testData

	testSeries := dataframe.NewSeriesFloat64("Test Data", nil, testData)

	var st, Yorigin float64
	// Training smoothing Level
	for i := start; i < end+1; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
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

	// building test forecast

	fcast := []float64{}
	for k := end + 1; k < len(em.data); k++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		st = α*Yorigin + (1-α)*st
		fcast = append(fcast, st)

	}

	fcastSeries := dataframe.NewSeriesFloat64("Forecast Data", nil)
	fcastSeries.Values = fcast
	em.fcastData = fcastSeries

	errOpts := &ErrorOptions{}

	var val float64

	if errTyp == MAE {
		val, _, err = MeanAbsoluteError(ctx, testSeries, fcastSeries, errOpts)
		if err != nil {
			return nil, err
		}
	} else if errTyp == SSE {
		val, _, err = SumOfSquaredErrors(ctx, testSeries, fcastSeries, errOpts)
		if err != nil {
			return nil, err
		}
	} else if errTyp == RMSE {
		val, _, err = RootMeanSquaredError(ctx, testSeries, fcastSeries, errOpts)
		if err != nil {
			return nil, err
		}
	} else if errTyp == MAPE {
		val, _, err = MeanAbsolutePercentageError(ctx, testSeries, fcastSeries, errOpts)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("Unknown error type")
	}

	em.errorM = &ErrorMeasurement{
		errorType: errTyp,
		value:     val,
	}

	return em, nil
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
