package forecast

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bradfitz/iter"
	dataframe "github.com/rocketlaunchr/dataframe-go"
	pd "github.com/rocketlaunchr/dataframe-go/pandas"
	"github.com/rocketlaunchr/dataframe-go/utils/utime"
)

// SesModel is a struct that holds necessary
// computed values for a forecasting result
type SesModel struct {
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

// SesFitOpts is used to set necessary parameters
// needed to run Fit on a Simple Exponential Smoothing Algorithm
type SesFitOpts struct {
	Alpha float64
}

// SimpleExponentialSmoothing Function receives a series data of type dataframe.Seriesfloat64
// It returns a SesModel from which Fit and Predict method can be carried out.
func SimpleExponentialSmoothing(ctx context.Context, s interface{}) *SesModel {
	var (
		data      []float64
		isDf      bool
		tsInt     string
		tReverse  bool
		err       error
		tsName    string
		lastTsVal time.Time
	)

	model := &SesModel{
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

	switch d := s.(type) {
	case *dataframe.SeriesFloat64:
		data = d.Values

	case *dataframe.DataFrame:
		isDf = true
		// validate that
		// - DataFrame has exactly two columns
		// - first column is SeriesTime
		// - second column is SeriesFloat64
		if len(d.Series) != 2 {

			panic("dataframe passed in must have exactly two series/columns.")
		} else {
			if d.Series[0].Type() != "time" {
				panic("first column/series must be a SeriesTime")
			} else { // get the current time interval/freq from the seriesTime
				if ts, ok := d.Series[0].(*dataframe.SeriesTime); ok {
					tsName = ts.Name(dataframe.DontLock)

					rowLen := ts.NRows(dataframe.DontLock)
					// store the last value in timeSeries column
					lastTsVal = ts.Value(rowLen-1, dataframe.DontLock).(time.Time)

					// guessing with only half the original time series row length
					half := rowLen / 2
					utimeOpts := utime.GuessTimeFreqOptions{
						R:        &dataframe.Range{End: &half},
						DontLock: true,
					}

					tsInt, tReverse, err = utime.GuessTimeFreq(ctx, ts, utimeOpts)
					if err != nil {
						panic(fmt.Errorf("error while trying to figure out interval for time series column: %v\n", err))
					}
				} else {
					panic("column 0 not convertible to SeriesTime")
				}
			}

			if d.Series[1].Type() != "float64" {
				panic("second column/series must be a SeriesFloat64")
			} else {
				val := d.Series[1].Copy()
				if v, ok := val.(*dataframe.SeriesFloat64); ok {
					data = v.Values
				} else {
					panic("column 1 not convertible to SeriesFloat64")
				}
			}
		}

	default:
		panic("unknown data format passed in. make sure you pass in a SeriesFloat64 or a forecast standard two(2) column dataframe.")
	}

	model.data = data
	if isDf {
		model.inputIsDf = isDf
		model.tsInterval = tsInt
		model.tsIntReverse = tReverse
		model.tsName = tsName
		model.lastTsVal = lastTsVal
	}

	return model
}

// Fit Method performs the splitting and trainging of the SesModel based on the Exponential Smoothing algorithm.
// It returns a trained SesModel ready to carry out future predictions.
// The argument α must be between [0,1].
func (sm *SesModel) Fit(ctx context.Context, tr *dataframe.Range, opts interface{}, et ...ErrorType) (*SesModel, error) {

	var (
		α      float64
		r      *dataframe.Range
		errTyp ErrorType
	)

	if o, ok := opts.(SesFitOpts); ok {

		α = o.Alpha

	} else {
		return nil, errors.New("fit options passed is not compartible with ses model")
	}

	if tr != nil {
		r = tr
	}

	if len(et) > 0 {
		errTyp = et[0]
	}

	count := len(sm.data)
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

	sm.alpha = α

	trainData := sm.data[start : end+1]
	sm.trainData = trainData

	testData := sm.data[end+1:]
	if len(testData) < 3 {
		return nil, errors.New("There should be a minimum of 3 data left as testing data")
	}
	sm.testData = testData

	testSeries := dataframe.NewSeriesFloat64("Test Data", nil, testData)

	var st, Yorigin float64
	// Training smoothing Level
	for i := start; i < end+1; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		xt := sm.data[i]

		if i == start {
			st = xt
			sm.initialLevel = xt

		} else if i == end { // Setting the last value in traindata as Yorigin value for bootstrapping
			Yorigin = sm.data[i]
			sm.originValue = Yorigin
		} else {
			st = α*xt + (1-α)*st
		}
	}
	sm.smoothingLevel = st

	// building test forecast

	fcast := []float64{}
	for k := end + 1; k < len(sm.data); k++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		st = α*Yorigin + (1-α)*st
		fcast = append(fcast, st)

	}

	fcastSeries := dataframe.NewSeriesFloat64("Forecast Data", nil)
	fcastSeries.Values = fcast
	sm.fcastData = fcastSeries

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

	sm.errorM = &ErrorMeasurement{
		errorType: errTyp,
		value:     val,
	}

	return sm, nil
}

// Predict method is used to run future predictions for Ses
// Using Ses Bootstrapping method
// It returns an interface{} result that is either dataframe.SeriesFloat64 or dataframe.Dataframe format
func (sm *SesModel) Predict(ctx context.Context, m int) (interface{}, error) {
	if m <= 0 {
		return nil, errors.New("m must be greater than 0")
	}

	forecast := make([]float64, m)
	α := sm.alpha
	Yorigin := sm.originValue
	st := sm.smoothingLevel

	// Now calculate forecast
	pos := 0
	for range iter.N(m) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		st = α*Yorigin + (1-α)*st
		forecast[pos] = st

		pos++
	}

	fdf := dataframe.NewSeriesFloat64("Prediction", nil)
	fdf.Values = forecast

	if sm.inputIsDf {

		size := m + 1

		// generate SeriesTime to start and continue from where it stopped in data input
		opts := utime.NewSeriesTimeOptions{
			Size: &size,
		}
		ts, err := utime.NewSeriesTime(ctx, sm.tsName, sm.tsInterval, sm.lastTsVal, sm.tsIntReverse, opts)
		if err != nil {
			panic(fmt.Errorf("error encountered while generating time interval prediction: %v\n", err))
		}

		// trying to exclude the first starting time
		nTs := ts.Copy(dataframe.Range{Start: &[]int{1}[0]})

		// combine fdf and generated time series into a dataframe and return
		return dataframe.NewDataFrame(nTs, fdf), nil
	}

	return fdf, nil
}

// Summary function is used to Print out Data Summary
// From the Trained Model
func (sm *SesModel) Summary() {
	// Display training info
	alpha := dataframe.NewSeriesFloat64("Alpha", nil, sm.alpha)
	initLevel := dataframe.NewSeriesFloat64("Initial Level", nil, sm.initialLevel)
	st := dataframe.NewSeriesFloat64("Smooting Level", nil, sm.smoothingLevel)

	info := dataframe.NewDataFrame(alpha, initLevel, st)
	fmt.Println(info.Table())

	// Display error Measurement info
	errTyp := sm.errorM.Type()
	errVal := sm.errorM.Value()
	errorM := dataframe.NewSeriesFloat64(errTyp, nil, errVal)

	fmt.Println(errorM.Table())

	// Display Test Data and Forecast data info
	testSeries := dataframe.NewSeriesFloat64("Test Data", nil, sm.testData)
	fmt.Println(testSeries.Table())

	fmt.Println(sm.fcastData.Table())
}

// Describe outputs various statistical information of testData or trainData Series in SesModel
func (sm *SesModel) Describe(ctx context.Context, typ DataType, opts ...pd.DescribeOptions) {
	var o pd.DescribeOptions

	if len(opts) > 0 {
		o = opts[0]
	}

	data := &dataframe.SeriesFloat64{}

	if typ == TrainData {
		trainSeries := dataframe.NewSeriesFloat64("Test Data", nil, sm.trainData)
		data = trainSeries
	} else if typ == TestData {
		testSeries := dataframe.NewSeriesFloat64("Test Data", nil, sm.testData)
		data = testSeries
	} else if typ == MainData {
		dataSeries := dataframe.NewSeriesFloat64("Complete Data", nil, sm.data)
		data = dataSeries
	} else {
		panic(errors.New("unrecognised data type selection specified"))
	}

	output, err := pd.Describe(ctx, data, o)
	if err != nil {
		panic(err)
	}
	fmt.Println(output)

	return
}
