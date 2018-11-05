package inou

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"github.com/kzoper/dataframe-go"
	"io"
	"io/ioutil"
)

type CSVOptions struct {
	Comma            rune
	Comment          rune
	TrimLeadingSpace bool
	LazyQuotes       bool
}

func From2DStringSlice(inp [][]string) (*dataframe.DataFrame, error) {
	var df *dataframe.DataFrame

	if len(inp) == 0 {
		return nil, errors.New("empty slice")
	}
	for key, line := range inp {
		if key == 0 {
			var series []dataframe.Series
			for _, value := range line {
				series = append(series, dataframe.NewSeriesString(value, nil))
			}
			df = dataframe.NewDataFrame(series...)
		} else {
			var values []interface{}
			for _, value := range line {
				values = append(values, value)
			}
			df.Append(values...)
		}

	}
	return df, nil
}

func To2DStringSlice(df *dataframe.DataFrame) [][]string{
	df.Lock()
	defer df.Unlock()
	var out [][]string
	numOfSeries := len(df.Series)
	numOfRows := df.Series[0].NRows()
	out = append(out, df.Names())
	for j := 0; j < numOfRows; j++ {
		var res []string
		for i := 0; i < numOfSeries; i++ {
			res = append(res, df.Series[i].ValueString(j))
		}
		out = append(out, res)
	}
	return out
}

func FromCSV(r io.Reader, options ...CSVOptions) (*dataframe.DataFrame, error) {
	var df *dataframe.DataFrame

	reader := csv.NewReader(r)
	reader.ReuseRecord = true
	if len(options) > 0 {
		reader.Comma = options[0].Comma
		reader.Comment = options[0].Comment
		reader.TrimLeadingSpace = options[0].TrimLeadingSpace
		reader.LazyQuotes = options[0].LazyQuotes
	}

	inputSlice,err := reader.ReadAll()
	if err != nil {
		return nil,err
	}

	df, err = From2DStringSlice(inputSlice)
	if err != nil {
		return nil, err
	}
	return df, nil
}

func ToCSV(df *dataframe.DataFrame,w io.Writer, options ...CSVOptions) {
	writer := csv.NewWriter(w)
	if len(options) > 0 {
		writer.Comma = options[0].Comma
	}
	defer writer.Flush()

	writer.WriteAll(To2DStringSlice(df))
}

func FromJSON(r io.Reader) (*dataframe.DataFrame, error) {
	var df *dataframe.DataFrame

	byteValue, _ := ioutil.ReadAll(r)

	var result [][]interface{}

	err := json.Unmarshal(byteValue, &result)
	if err != nil {
		return nil, err
	}

	var inputSlice [][]string
	for _, line := range result {
		var lineSlice []string
		for _, value := range line {
			lineSlice = append(lineSlice, value.(string))
		}
		inputSlice = append(inputSlice, lineSlice)
	}

	df, err = From2DStringSlice(inputSlice)
	if err != nil {
		return nil, err
	}

	return df, nil
}

func ToJSON(df *dataframe.DataFrame,w io.Writer) {
	bytes, _ := json.Marshal(To2DStringSlice(df))

	w.Write(bytes)
}