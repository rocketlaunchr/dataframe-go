package imports

import (
	"encoding/csv"
	"errors"
	// "fmt"
	"io"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// CSVLoadOption is likely to change.
type CSVLoadOption struct {
	Comma            rune
	Comment          rune
	TrimLeadingSpace bool
}

// LoadFromCSV will load data from a csv file.
// API is likely to change.
func LoadFromCSV(r io.Reader, options ...CSVLoadOption) (*dataframe.DataFrame, error) {

	seriess := []dataframe.Series{}
	var df *dataframe.DataFrame

	cr := csv.NewReader(r)
	cr.ReuseRecord = true
	if len(options) > 0 {
		cr.Comma = options[0].Comma
		cr.Comment = options[0].Comment
		cr.TrimLeadingSpace = options[0].TrimLeadingSpace
	}

	var count int
	for {
		rec, err := cr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if count == 0 {
			// Create the series
			for _, name := range rec {
				seriess = append(seriess, dataframe.NewSeriesString(name, nil))
			}
			df = dataframe.NewDataFrame(seriess...)
		} else {
			vals := []interface{}{}
			for _, v := range rec {
				vals = append(vals, v)
			}
			df.Append(vals...)
		}
		count++
	}

	if count < 1 {
		return nil, errors.New("csv data contains no rows")
	}

	return dataframe.NewDataFrame(seriess...), nil
}

// JSONLoadOption is likely to change.
type JSONLoadOption struct {
}

// LoadFromJSON will load data from a json file.
// The function is not implemented. Pull-request?
func LoadFromJSON(r io.Reader, options ...JSONLoadOption) (*dataframe.DataFrame, error) {
	panic("LoadFromJSON: TODO")
}
