// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"context"
	"strings"
	"testing"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

var ctx = context.Background()

func TestCSVImport(t *testing.T) {

	csvStr := `
Country,Date,Age,Amount,Id
"United States",2012-02-01,50,112.1,01234
"United States",2012-02-01,32,321.31,54320
"United Kingdom",2012-02-01,17,18.2,12345
"United States",2012-02-01,32,321.31,54320
"United Kingdom",2015-05-07,NA,18.2,12345
"United States",2012-02-01,32,321.31,54320
"United States",2012-02-01,32,321.31,54320
Spain,2012-02-01,66,555.42,00241
`

	opts := CSVLoadOptions{
		InferDataTypes: true,
		NilValue:       &[]string{"NA"}[0],
		DictateDataType: map[string]interface{}{
			"Id": float64(0),
		},
	}

	df, err := LoadFromCSV(ctx, strings.NewReader(csvStr), opts)
	if err != nil {
		t.Errorf("csv import error: %v", err)
		return
	}

	// Expected solution
	parseTime := func(s string) time.Time {
		t, _ := time.Parse("2006-01-02", s)
		return t
	}

	expDf := dataframe.NewDataFrame(
		dataframe.NewSeriesString("country", nil, "United States", "United States", "United Kingdom", "United States", "United Kingdom", "United States", "United States", "Spain"),
		dataframe.NewSeriesTime("date", nil, parseTime("2012-02-01"), parseTime("2012-02-01"), parseTime("2012-02-01"), parseTime("2012-02-01"), parseTime("2015-05-07"), parseTime("2012-02-01"), parseTime("2012-02-01"), parseTime("2012-02-01")),
		dataframe.NewSeriesInt64("age", nil, 50, 32, 17, 32, nil, 32, 32, 66),
		dataframe.NewSeriesFloat64("amount", nil, 112.1, 321.31, 18.2, 321.31, 18.2, 321.31, 321.31, 555.42),
		dataframe.NewSeriesFloat64("id", nil, 1234, 54320, 12345, 54320, 12345, 54320, 54320, 241),
	)

	if eq, _ := df.IsEqual(ctx, expDf); !eq {
		t.Errorf("csv import not equal")
	}
}
