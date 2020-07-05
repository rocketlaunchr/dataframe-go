package imports

import (
	"github.com/rocketlaunchr/dataframe-go"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestLoadFromCSV(t *testing.T) {
	csvStr := `
Country,Time,Date,Age,Amount,Id
"United States",11:12,2012-02-01,50,112.1,01234
"United States",01:13,2012-02-01,32,321.31,54320
"United Kingdom",13:20,2012-02-01,17,18.2,12345
"United States",15:07,2012-02-01,32,321.31,54320
"United Kingdom",09:34,2015-05-07,NA,18.2,12345
"United States",20:22,2012-02-01,32,321.31,54320
"United States",23:15,2012-02-01,32,321.31,54320
Spain,19:56,2012-02-01,66,555.42,00241
`

	opts := CSVLoadOptions{
		InferDataTypes: true,
		NilValue:       &[]string{"NA"}[0],
		DictateDataType: map[string]interface{}{
			"Id":       float64(0),
			"Datetime": time.Time{},
		},
		MergeColumns: map[string][]string{
			"Datetime": {"Date", "Time"},
		},
		TimeFormat: "2006-01-02 15:04",
	}

	df, err := LoadFromCSV(ctx, strings.NewReader(csvStr), opts)
	if err != nil {
		t.Errorf("csv import error: %v", err)
		t.FailNow()
	}

	// Expected solution
	parseTime := func(s string) time.Time {
		t, _ := time.Parse(opts.TimeFormat, s)
		return t
	}

	expDf := dataframe.NewDataFrame(
		dataframe.NewSeriesString("country", nil, "United States", "United States", "United Kingdom", "United States", "United Kingdom", "United States", "United States", "Spain"),
		dataframe.NewSeriesInt64("age", nil, 50, 32, 17, 32, nil, 32, 32, 66),
		dataframe.NewSeriesFloat64("amount", nil, 112.1, 321.31, 18.2, 321.31, 18.2, 321.31, 321.31, 555.42),
		dataframe.NewSeriesFloat64("id", nil, 1234, 54320, 12345, 54320, 12345, 54320, 54320, 241),
		dataframe.NewSeriesTime("datetime", nil,
			parseTime("2012-02-01 11:12"),
			parseTime("2012-02-01 01:13"),
			parseTime("2012-02-01 13:20"),
			parseTime("2012-02-01 15:07"),
			parseTime("2015-05-07 09:34"),
			parseTime("2012-02-01 20:22"),
			parseTime("2012-02-01 23:15"),
			parseTime("2012-02-01 19:56"),
		),
	)

	if eq, _ := df.IsEqual(ctx, expDf); !eq {
		t.Errorf("csv import not equal")
	}
}

func TestLoadFromCSV_invalid_merge_columns_options(t *testing.T) {
	csvStr := `
Country,Time,Date,Age,Amount,Id
"United States",11:12,2012-02-01,50,112.1,01234
"United States",01:13,2012-02-01,32,321.31,54320
"United Kingdom",13:20,2012-02-01,17,18.2,12345
"United States",15:07,2012-02-01,32,321.31,54320
"United Kingdom",09:34,2015-05-07,NA,18.2,12345
"United States",20:22,2012-02-01,32,321.31,54320
"United States",23:15,2012-02-01,32,321.31,54320
Spain,19:56,2012-02-01,66,555.42,00241
`

	opts := CSVLoadOptions{
		InferDataTypes: true,
		NilValue:       &[]string{"NA"}[0],
		DictateDataType: map[string]interface{}{
			"Id":       float64(0),
			"Datetime": time.Time{},
		},
		MergeColumns: map[string][]string{
			"Datetime": {"Date", "Invalid"},
		},
		TimeFormat: "2006-01-02 15:04",
	}

	df, err := LoadFromCSV(ctx, strings.NewReader(csvStr), opts)
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "merging into column"))
	assert.Nil(t, df)
}
