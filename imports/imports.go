// Copyright 2018 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/davecgh/go-spew/spew"
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
	// LargeDataSet should be set to true for large datasets.
	// It will set the capacity of the underlying slices of the dataframe by performing a basic parse
	// of the full dataset before processing the data fully.
	// Preallocating memory can provide speed improvements. Benchmarks should be performed for your use-case.
	LargeDataSet bool

	// DictateDataType is used to inform LoadFromJSON what the true underlying data type is for a given field name.
	// The value for a given key must be of the data type of the data. For a string use "". For a int64 use int64(0).
	DictateDataType map[string]interface{}

	// ErrorOnUnknownFields will generate an error if an unknown field is encountered after the first row.
	ErrorOnUnknownFields bool
}

// LoadFromJSON will load data from a json file.
// The function is not implemented. Pull-request?
func LoadFromJSON(r io.ReadSeeker, options ...JSONLoadOption) (*dataframe.DataFrame, error) {

	init := &dataframe.SeriesInit{}

	if len(options) > 0 {
		// Count how many rows we have in order to preallocate
		if options[0].LargeDataSet {
			dec := json.NewDecoder(r)

			tokenCount := 0
			for {

				t, err := dec.Token()
				if err != nil {
					if err == io.EOF {
						r.Seek(0, io.SeekStart)
						break
					}
					return nil, err
				}

				switch delim := t.(type) {
				case json.Delim:
					if delim.String() == "{" {
						tokenCount++
					} else if delim.String() == "}" {
						tokenCount--
						if tokenCount == 0 {
							init.Size++
							// init.Capacity++
						}
					}
				}
			}
		}
	}

	var row int
	var df *dataframe.DataFrame

	dec := json.NewDecoder(r)
	dec.UseNumber()
	for {
		var raw map[string]interface{}
		err := dec.Decode(&raw)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		row++

		vals := parseObject(raw, "")

		if row == 1 {
			// Create the initial dataframe
			seriess := []dataframe.Series{}
			nameToSeriesIdx := map[string]int{}

			// Initialize the underlying series of dataframe.
			if len(options) > 0 && len(options[0].DictateDataType) > 0 {
				for k, v := range options[0].DictateDataType {
					switch v.(type) {
					case float64:
						seriess = append(seriess, dataframe.NewSeriesFloat64(k, init))
					case int64:
						seriess = append(seriess, dataframe.NewSeriesInt64(k, init))
					case string:
						seriess = append(seriess, dataframe.NewSeriesString(k, init))
					case time.Time:
						seriess = append(seriess, dataframe.NewSeriesTime(k, init))
					default:
						seriess = append(seriess, dataframe.NewSeries(k, v, init))
					}
					nameToSeriesIdx[k] = len(seriess) - 1
				}
			}

			for name, val := range vals {

				var strVal string
				switch v := val.(type) {
				case fmt.Stringer:
					strVal = v.String()
				default:
					strVal = fmt.Sprintf("%v", v)
				}

				// Check if the series exists
				colIdx, exists := nameToSeriesIdx[name]
				if !exists {
					seriess = append(seriess, dataframe.NewSeriesString(name, init, strVal))
					continue
				}

				seriess[colIdx].Update(row-1, strVal, dataframe.Options{DontLock: true})
			}

			// Number can be float64, int64, string

			// for name, val := range vals {
			// 	switch v := val.(type) {
			// 	case string:
			// 		seriess = append(seriess, dataframe.NewSeriesString(name, init, v))
			// 	case json.Number:

			// 	default:

			// 	}
			// }
			df = dataframe.NewDataFrame(seriess...)
		} else {

			for name, val := range vals {
				// Check if the series exists
				col, err := df.NameToColumn(name)
				if err != nil {
					if len(options) > 0 && options[0].ErrorOnUnknownFields {
						return nil, fmt.Errorf("unknown field encountered. row: %d field: %s", row-1, name)
					}
					continue
				}
				df.Update(row-1, col, val)
			}

		}

		fmt.Println(spew.Sdump(vals))
	}

	return df, nil
}

func parseObject(v map[string]interface{}, prefix string) map[string]interface{} {

	out := map[string]interface{}{}

	for k, t := range v {
		var key string
		if prefix == "" {
			key = k
		} else {
			key = prefix + "." + k
		}

		switch v := t.(type) {
		case map[string]interface{}:
			for k, t := range parseObject(v, key) {
				out[k] = t
			}
		default:
			out[key] = t
		}
	}

	return out
}
