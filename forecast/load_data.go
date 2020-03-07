package forecast

import (
	"context"
	"errors"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/utils/utime"
)

func loadDataFromDF(ctx context.Context, model Algorithm, d *dataframe.DataFrame) error {
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

	switch m := model.(type) {
	case *ExponentialSmoothing:
		m.data = data
		m.inputIsDf = isDf
		m.ts.tsInterval = tsInt
		m.ts.tsIntReverse = tReverse
		m.ts.tsName = tsName
		m.ts.lastTsVal = lastTsVal

	default:
		return errors.New("Unsupported Model passed")
	}

	return nil
}

func splitModelData(model Algorithm, start, end int) error {
	var (
		trainData, testData []float64
	)
	switch m := model.(type) {
	case *ExponentialSmoothing:
		trainData = m.data[start : end+1]
		m.trainData = trainData

		testData = m.data[end+1:]
		if len(testData) < 3 {
			return errors.New("there should be a minimum of 3 data left as testing data")
		}
		m.testData = testData
	}

	return nil
}
