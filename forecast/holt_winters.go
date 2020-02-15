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

// HwModel is a Model Interface that holds necessary
// computed values for a forecasting result
type HwModel struct {
	data                 []float64
	trainData            []float64
	testData             []float64
	fcastData            *dataframe.SeriesFloat64
	initialSmooth        float64
	initialTrend         float64
	initialSeasonalComps []float64
	smoothingLevel       float64
	trendLevel           float64
	seasonalComps        []float64
	period               int
	alpha                float64
	beta                 float64
	gamma                float64
	errorM               *ErrorMeasurement
	inputIsDf            bool
	tsInterval           string
	tsIntReverse         bool
	tsName               string
	lastTsVal            time.Time
}

// HwFitOpts is used to set necessary parameters
// needed to run Fit on Holt Winters Algorithm
type HwFitOpts struct {
	Alpha  float64
	Beta   float64
	Gamma  float64
	Period int
}

// HoltWinters Function receives a series data of type dataframe.Seriesfloat64
// It returns a HwModel from which Fit and Predict method can be carried out.
func HoltWinters(ctx context.Context, s interface{}) *HwModel {
	var (
		data      []float64
		isDf      bool
		tsInt     string
		tReverse  bool
		err       error
		tsName    string
		lastTsVal time.Time
	)

	model := &HwModel{
		data:                 []float64{},
		trainData:            []float64{},
		testData:             []float64{},
		fcastData:            &dataframe.SeriesFloat64{},
		initialSmooth:        0.0,
		initialTrend:         0.0,
		initialSeasonalComps: []float64{},
		smoothingLevel:       0.0,
		trendLevel:           0.0,
		seasonalComps:        []float64{},
		period:               0,
		alpha:                0.0,
		beta:                 0.0,
		gamma:                0.0,
		errorM:               &ErrorMeasurement{},
		inputIsDf:            isDf,
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
		panic("unknown data format passed in. make sure you pass in a SeriesFloat64 or standard two(2) column dataframe")
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

// Fit Method performs the splitting and trainging of the HwModel based on the Tripple Exponential Smoothing algorithm.
// It returns a trained HwModel ready to carry out future predictions.
// The arguments α, beta nd gamma must be between [0,1]. Recent values receive more weight when α is closer to 1.
func (hm *HwModel) Fit(ctx context.Context, tr *dataframe.Range, opts interface{}, et ...ErrorType) (*HwModel, error) {

	var (
		α, β, γ float64
		period  int
		r       *dataframe.Range
		errTyp  ErrorType
	)

	if o, ok := opts.(HwFitOpts); ok {

		α = o.Alpha
		β = o.Beta
		γ = o.Gamma
		period = o.Period

	} else {
		return nil, errors.New("fit options passed is not compartible with holtwinters model")
	}

	if tr != nil {
		r = tr
	}

	if len(et) > 0 {
		errTyp = et[0]
	}

	start, end, err := r.Limits(len(hm.data))
	if err != nil {
		return nil, err
	}

	// Validation
	if end-start < 1 {
		return nil, errors.New("no values in series range")
	}

	if (α < 0.0) || (α > 1.0) {
		return nil, errors.New("α must be between [0,1]")
	}

	if (β < 0.0) || (β > 1.0) {
		return nil, errors.New("β must be between [0,1]")
	}

	if (γ < 0.0) || (γ > 1.0) {
		return nil, errors.New("γ must be between [0,1]")
	}

	trainData := hm.data[start : end+1]
	hm.trainData = trainData

	testData := hm.data[end+1:]
	if len(testData) < 3 {
		return nil, errors.New("There should be a minimum of 3 data left as testing data")
	}
	hm.testData = testData

	testSeries := dataframe.NewSeriesFloat64("Test Data", nil, testData)

	hm.alpha = α
	hm.beta = β
	hm.gamma = γ

	y := hm.data[start : end+1]

	seasonals := initialSeasonalComponents(y, period)

	hm.initialSeasonalComps = initialSeasonalComponents(y, period)

	var trnd, prevTrnd float64
	trnd = initialTrend(y, period)
	hm.initialTrend = trnd

	var st, prevSt float64 // smooth

	for i := start; i < end+1; i++ {
		// Breaking out on context failure
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		xt := y[i]

		if i == start { // Set initial smooth
			st = xt

			hm.initialSmooth = xt

		} else {
			// multiplicative method
			// prevSt, st = st, α * (xt / seasonals[i % period]) + (1 - α) * (st + trnd)
			// prevTrnd, trnd = trnd, β * (st - prevSt) + (1 - β) * trnd
			// seasonals[i % period] = γ * (xt / (prevSt + prevTrnd)) + (1 - γ) * seasonals[i % period]

			// additive method
			prevSt, st = st, α*(xt-seasonals[i%period])+(1-α)*(st+trnd)
			prevTrnd, trnd = trnd, β*(st-prevSt)+(1-β)*trnd
			seasonals[i%period] = γ*(xt-prevSt-prevTrnd) + (1-γ)*seasonals[i%period]
			// _ = prevTrnd
			// fmt.Println(st + trnd + seasonals[i % period])
		}

	}

	// This is for the test forecast
	fcast := []float64{}
	m := 1
	for k := end + 1; k < len(hm.data); k++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// multiplicative Method
		// fcast = append(fcast, (st + float64(m)*trnd) * seasonals[(m-1) % period])

		// additive method
		fcast = append(fcast, (st+float64(m)*trnd)+seasonals[(m-1)%period])

		m++
	}

	fcastSeries := dataframe.NewSeriesFloat64("Forecast Data", nil)
	fcastSeries.Values = fcast
	hm.fcastData = fcastSeries

	hm.smoothingLevel = st
	hm.trendLevel = trnd
	hm.period = period
	hm.seasonalComps = seasonals

	// NOw to calculate the Errors
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

	hm.errorM = &ErrorMeasurement{
		errorType: errTyp,
		value:     val,
	}

	return hm, nil
}

// Predict method runs future predictions for HoltWinter Model
// It returns an interface{} result that is either dataframe.SeriesFloat64 or dataframe.Dataframe format
func (hm *HwModel) Predict(ctx context.Context, h int) (interface{}, error) {

	// Validation
	if h <= 0 {
		return nil, errors.New("value of h must be greater than 0")
	}

	forecast := make([]float64, h)

	st := hm.smoothingLevel
	seasonals := hm.seasonalComps
	trnd := hm.trendLevel
	period := hm.period

	m := 1
	pos := 0
	for range iter.N(h) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// multiplicative Method
		// fcast = append(fcast, (st + float64(m)*trnd) * seasonals[(m-1) % period])

		// additive method
		forecast[pos] = (st + float64(m)*trnd) + seasonals[(m-1)%period]

		m++
		pos++
	}

	fdf := dataframe.NewSeriesFloat64("Prediction", nil)
	fdf.Values = forecast

	if hm.inputIsDf {
		size := h + 1

		// generate SeriesTime to start and continue from where it stopped in data input
		opts := utime.NewSeriesTimeOptions{
			Size: &size,
		}
		ts, err := utime.NewSeriesTime(ctx, hm.tsName, hm.tsInterval, hm.lastTsVal, hm.tsIntReverse, opts)
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
func (hm *HwModel) Summary() {
	// Display training info
	alpha := dataframe.NewSeriesFloat64("Alpha", nil, hm.alpha)
	beta := dataframe.NewSeriesFloat64("Beta", nil, hm.beta)
	gamma := dataframe.NewSeriesFloat64("Gamma", nil, hm.gamma)
	period := dataframe.NewSeriesFloat64("Period", nil, hm.period)

	infoConstants := dataframe.NewDataFrame(alpha, beta, gamma, period)
	fmt.Println(infoConstants.Table())

	initSmooth := dataframe.NewSeriesFloat64("Initial Smooothing Level", nil, hm.initialSmooth)
	initTrend := dataframe.NewSeriesFloat64("Initial Trend Level", nil, hm.initialTrend)

	st := dataframe.NewSeriesFloat64("Smooting Level", nil, hm.smoothingLevel)
	trnd := dataframe.NewSeriesFloat64("Trend Level", nil, hm.trendLevel)

	infoComponents := dataframe.NewDataFrame(initSmooth, initTrend, st, trnd)
	fmt.Println(infoComponents.Table())

	initSeasonalComps := dataframe.NewSeriesFloat64("Initial Seasonal Components", nil)
	initSeasonalComps.Values = hm.initialSeasonalComps

	seasonalComps := dataframe.NewSeriesFloat64("Trained Seasonal Components", nil)
	seasonalComps.Values = hm.seasonalComps

	fmt.Println(initSeasonalComps.Table())
	fmt.Println(seasonalComps.Table())

	// Display error Measurement info
	errTyp := hm.errorM.Type()
	errVal := hm.errorM.Value()
	errorM := dataframe.NewSeriesFloat64(errTyp, nil, errVal)

	fmt.Println(errorM.Table())

	// Display Test Data and Forecast data info
	testSeries := dataframe.NewSeriesFloat64("Test Data", nil, hm.testData)
	fmt.Println(testSeries.Table())

	fmt.Println(hm.fcastData.Table())
}

// Describe outputs various statistical information of testData or trainData Series in HwModel
func (hm *HwModel) Describe(ctx context.Context, typ DataType, opts ...pd.DescribeOptions) {
	var o pd.DescribeOptions

	if len(opts) > 0 {
		o = opts[0]
	}

	data := &dataframe.SeriesFloat64{}

	if typ == TrainData {
		trainSeries := dataframe.NewSeriesFloat64("Train Data", nil, hm.trainData)
		data = trainSeries
	} else if typ == TestData {
		testSeries := dataframe.NewSeriesFloat64("Test Data", nil, hm.testData)
		data = testSeries
	} else if typ == MainData {
		dataSeries := dataframe.NewSeriesFloat64("Complete Data", nil, hm.data)
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
