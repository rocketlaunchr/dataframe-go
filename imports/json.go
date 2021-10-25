// Copyright 2018-21 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/goccy/go-json"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// JSONLoadOptions is likely to change.
type JSONLoadOptions struct {

	// LargeDataSet should be set to true for large datasets.
	// It will set the capacity of the underlying slices of the Dataframe by performing a basic parse
	// of the full dataset before processing the data fully.
	// Preallocating memory can provide speed improvements. Benchmarks should be performed for your use-case.
	LargeDataSet bool

	// DictateDataType is used to inform LoadFromJSON what the true underlying data type is for a given field name.
	// The key must be the case-sensitive field name.
	// The value for a given key must be of the data type of the data.
	// eg. For a string use "". For an int64 use int64(0). What is relevant is the data type and not the value itself.
	//
	// NOTE: A custom Series must implement NewSerieser interface and be able to interpret strings to work.
	DictateDataType map[string]interface{}

	// ErrorOnUnknownFields will generate an error if an unknown field is encountered after the first row.
	ErrorOnUnknownFields bool

	// Path sets the location of the array containing the data to import. It uses dot notation relative to the root
	// JSON object. For JSONL files, it does nothing.
	//
	// NOTE: Not implemented.
	Path string
}

// LoadFromJSON will load data from a jsonl file or a JSON array.
// The first row determines which fields will be imported for subsequent rows.
//
// See: https://jsonlines.org for details on the file format.
func LoadFromJSON(ctx context.Context, r io.ReadSeeker, options ...JSONLoadOptions) (*dataframe.DataFrame, error) {

	var init *dataframe.SeriesInit

	if len(options) > 0 {
		// Count how many rows we have in order to preallocate underlying slices
		if options[0].LargeDataSet {
			init = &dataframe.SeriesInit{}

			var (
				openRune  json.Delim
				closeRune json.Delim
			)

			// Count how many rows in the data
			fmt, err := detectJSONDataFormat(r)
			if err != nil {
				return nil, err
			}

			if fmt == jsonlArray {
				openRune = '['
				closeRune = ']'
			} else {
				openRune = '{'
				closeRune = '}'
			}

			dec := json.NewDecoder(r)

			tokenCount := 0
			for {
				if err := ctx.Err(); err != nil {
					return nil, err
				}

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
					if delim == openRune {
						tokenCount++
					} else if delim == closeRune {
						tokenCount--
						if tokenCount == 0 {
							init.Capacity++
						}
					}
				}
			}

			// For jsonlArray, the first array is for headings
			if fmt == jsonlArray && init.Capacity > 0 {
				init.Capacity--
			}
		}
	}

	var df *dataframe.DataFrame

	rowIter, jf, err := readJSON(r)
	if err != nil {
		return nil, err
	}

	nameToIdx := map[string]int{} // map series name to index in df

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		row, _rowVals, err := rowIter()
		if err != nil {
			return nil, err
		}
		if row == nil {
			break
		}

		var rowVals map[string]interface{}

		if *row == 0 {
			// STEP 1: Determine Series types for Dataframe

			// Create a series for each field (of the appropriate data type)
			seriess := []dataframe.Series{}
			var colNames []string

			if jf == jsonlArray {
				// We are provided the series names already
				colNames = _rowVals.([]interface{})[0].([]string)
				rowVals = _rowVals.([]interface{})[1].(map[string]interface{})
			} else {
				rowVals = _rowVals.(map[string]interface{})
				for k := range rowVals {
					colNames = append(colNames, k)
				}
			}

			for _, name := range colNames {
				// Check if the datatype has been dictated.
				if len(options) > 0 && len(options[0].DictateDataType) > 0 {
					typ, exists := options[0].DictateDataType[name]
					if exists {
						// Datatype is dictated
						switch T := typ.(type) {
						case nil:
							panic("invalid dictated datatype for " + name)
						case float64:
							seriess = append(seriess, dataframe.NewSeriesFloat64(name, init))
						case int, int64, bool:
							seriess = append(seriess, dataframe.NewSeriesInt64(name, init))
						case string:
							seriess = append(seriess, dataframe.NewSeriesString(name, init))
						case time.Time:
							seriess = append(seriess, dataframe.NewSeriesTime(name, init))
						case dataframe.NewSerieser:
							seriess = append(seriess, T.NewSeries(name, init))
						case Converter:
							switch T.ConcreteType.(type) {
							case time.Time:
								seriess = append(seriess, dataframe.NewSeriesTime(name, init))
							default:
								seriess = append(seriess, dataframe.NewSeriesGeneric(name, T.ConcreteType, init))
							}
						default:
							seriess = append(seriess, dataframe.NewSeriesGeneric(name, typ, init))
						}
						continue
					}
				}

				// Not dictated, so determine data type from actual values
				switch rowVals[name].(type) {
				case nil:
					seriess = append(seriess, dataframe.NewSeriesString(name, init))
				case bool:
					seriess = append(seriess, dataframe.NewSeriesInt64(name, init))
				case string:
					seriess = append(seriess, dataframe.NewSeriesString(name, init))
				case json.Number:
					// Assume float
					seriess = append(seriess, dataframe.NewSeriesFloat64(name, init))
				default:
					return nil, errors.New("row: 0 - array or object detected for value")
				}
			}
			// Create the dataframe
			if len(seriess) == 0 {
				return nil, dataframe.ErrNoRows
			}
			for i, v := range seriess {
				nameToIdx[v.Name(dataframe.DontLock)] = i
			}
			df = dataframe.NewDataFrame(seriess...)
		} else {
			// case: row != 0
			rowVals = _rowVals.(map[string]interface{})
		}

		// STEP 2: Insert data into new row (faster to load data manually)

		// Create empty row
		df.Append(&dataframe.DontLock, make([]interface{}, len(df.Series))...)

		// Load data
		for name, val := range rowVals {
			idx, exists := nameToIdx[name]

			var insertVal interface{} = val

			if len(options) > 0 && len(options[0].DictateDataType) > 0 {
				typ, exists := options[0].DictateDataType[name]
				if exists {
					insertVal, err = dictateForce(*row, name, typ, val)
					if err != nil {
						return nil, err
					}
					goto STORE_VALUE
				}
			}

			switch v := val.(type) {
			case nil, bool, string, int, float64, int64:
				insertVal = v
			case json.Number:
				x, err := v.Float64()
				if err != nil {
					return nil, fmt.Errorf("row: %d - invalid value for %s", *row, name)
				}
				insertVal = x
			default:
				return nil, fmt.Errorf("row: %d - array or object detected for value", *row)
			}

		STORE_VALUE:
			if jf == jsonlArray {
				df.Series[idx].Update(*row, insertVal, dataframe.DontLock)
			} else {
				if !exists {
					// unknown field
					if len(options) > 0 && options[0].ErrorOnUnknownFields {
						return nil, fmt.Errorf("unknown field encountered. row: %d field: %s", *row, name)
					}
					continue
				}
				df.Series[idx].Update(*row, insertVal, dataframe.DontLock)
			}
		}
	}

	if df == nil {
		return nil, dataframe.ErrNoRows
	}

	// The order is not stable
	if jf != jsonlArray {
		names := df.Names(dataframe.DontLock)
		sort.Strings(names)
		df.ReorderColumns(names)
	}

	return df, nil
}
