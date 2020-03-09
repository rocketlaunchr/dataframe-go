package forecast

import (
	"context"
	"errors"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/utils/utime"
)

func loadDataFromDF(ctx context.Context, model Algorithm, d *dataframe.DataFrame, dataCol, tsCol interface{}) error {
	var (
		data      []float64
		ts        *dataframe.SeriesTime
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

	if dataCol != nil {
		switch dc := dataCol.(type) {
		case int:
			data = d.Series[dc].(*dataframe.SeriesFloat64).Values
		case string:
			i, err := d.NameToColumn(dc, dataframe.DontLock)
			if err != nil {
				return err
			}
			data = d.Series[i].(*dataframe.SeriesFloat64).Values
		}

	} else {
		switch df := d.Series[1].(type) {
		case *dataframe.SeriesFloat64:
			data = df.Values

		default:
			return errors.New("either set 'DataCol' in config or make sure second column is a float64 series")
		}
	}

	if tsCol != nil {
		switch tc := tsCol.(type) {
		case int:
			ts = d.Series[tc].(*dataframe.SeriesTime)
		case string:
			i, err := d.NameToColumn(tc, dataframe.DontLock)
			if err != nil {
				return err
			}
			ts = d.Series[i].(*dataframe.SeriesTime)
		}

	} else {
		switch t := d.Series[0].(type) {
		case *dataframe.SeriesTime:
			ts = t

		default:
			return errors.New("either set 'TsCol' in config or make sure first column is a time series")
		}
	}

	// get the current time interval/freq from the seriesTime

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

	switch m := model.(type) {
	case *ExponentialSmoothing:
		m.data = data
		m.inputIsDf = isDf
		m.ts.tsInterval = tsInt
		m.ts.tsIntReverse = tReverse
		m.ts.tsName = tsName
		m.ts.lastTsVal = lastTsVal

	default:
		return errors.New("unsupported model passed in")
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
