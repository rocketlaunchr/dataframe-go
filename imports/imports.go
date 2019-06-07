// Copyright 2018 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// CSVLoadOptions is likely to change.
type CSVLoadOptions struct {

	// Comma is the field delimiter.
	// The default value is ',' when CSVLoadOption is not provided.
	// Comma must be a valid rune and must not be \r, \n,
	// or the Unicode replacement character (0xFFFD).
	Comma rune

	// Comment, if not 0, is the comment character. Lines beginning with the
	// Comment character without preceding whitespace are ignored.
	// With leading whitespace the Comment character becomes part of the
	// field, even if TrimLeadingSpace is true.
	// Comment must be a valid rune and must not be \r, \n,
	// or the Unicode replacement character (0xFFFD).
	// It must also not be equal to Comma.
	Comment rune

	// If TrimLeadingSpace is true, leading white space in a field is ignored.
	// This is done even if the field delimiter, Comma, is white space.
	TrimLeadingSpace bool

	// LargeDataSet should be set to true for large datasets.
	// It will set the capacity of the underlying slices of the dataframe by performing a basic parse
	// of the full dataset before processing the data fully.
	// Preallocating memory can provide speed improvements. Benchmarks should be performed for your use-case.
	LargeDataSet bool

	// DictateDataType is used to inform LoadFromJSON what the true underlying data type is for a given field name.
	// The value for a given key must be of the data type of the data. For a string use "". For a int64 use int64(0).
	DictateDataType map[string]interface{}
}

// LoadFromCSV will load data from a csv file.
// WARNING: The API may change in the future.
func LoadFromCSV(r io.ReadSeeker, options ...CSVLoadOptions) (*dataframe.DataFrame, error) {

	seriess := []dataframe.Series{}
	var df *dataframe.DataFrame
	var init *dataframe.SeriesInit

	// Array to contain Field Names
	fieldNames := []string{}

	cr := csv.NewReader(r)
	cr.ReuseRecord = true
	if len(options) > 0 {
		cr.Comma = options[0].Comma
		cr.Comment = options[0].Comment
		cr.TrimLeadingSpace = options[0].TrimLeadingSpace

		// Count how many rows we have in order to preallocate underlying slices
		if options[0].LargeDataSet {
			init = &dataframe.SeriesInit{}
			for {
				_, err := cr.Read()
				if err != nil {
					if err == io.EOF {
						r.Seek(0, io.SeekStart)
						break
					}
					return nil, err
				}
				init.Size++
			}
			if init.Size > 0 {
				init.Size-- // Remove the space allocated for the "heading"
			}
		}
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

		// Read all field Nmes from First row
		if count == 0 { // Initializing Dataframe with field names
			// Create the series
			for _, name := range rec {
				// Store name in fieldNames array
				fieldNames = append(fieldNames, name)

				// If DictateDataType option is set
				if len(options) > 0 && len(options[0].DictateDataType) > 0 {
					// Check is name is defined in DictateDataType
					typ, exists := options[0].DictateDataType[name]
					if !exists { // If not defined Create the DF field as default string type
						seriess = append(seriess, dataframe.NewSeriesString(name, init))
						continue
					}

					switch typ.(type) {
					case float64:
						seriess = append(seriess, dataframe.NewSeriesFloat64(name, init))
					case int64:
						seriess = append(seriess, dataframe.NewSeriesInt64(name, init))
					case string:
						seriess = append(seriess, dataframe.NewSeriesString(name, init))
					case time.Time:
						seriess = append(seriess, dataframe.NewSeriesTime(name, init))
					default:
						seriess = append(seriess, dataframe.NewSeries(name, typ, init))
					}
				} else {
					// create field as String
					seriess = append(seriess, dataframe.NewSeriesString(name, init))
				}

			}
			df = dataframe.NewDataFrame(seriess...)
		} else { // Inserting of values to created Dataframe
			vals := []interface{}{}

			for index, v := range rec {

				// vals is supposed to be casted to the correct type by DictateDataType
				// If DictateDataType option is set
				if len(options) > 0 && len(options[0].DictateDataType) > 0 {
					// Check current column index has a dictated datatype
					typ, exists := options[0].DictateDataType[fieldNames[index]]
					// If column field type is not declared
					if !exists { // If field is not defined
						// Store input value as Default String
						vals = append(vals, v)
					} else { // If Field type is defined
						// Type cast String 'v' to whatever type that was defined
						switch typ.(type) { // switching based on defined type
						case float64:
							// Convert String V to float64
							val, err := strconv.ParseFloat(v, 64)
							if err != nil {
								fmt.Printf("can't force string to float64. row: %d field: %s", count-1, fieldNames[index])
								return nil, err
							}
							vals = append(vals, val)
						case int64:
							// Convert String v to int64
							val, err := strconv.ParseInt(v, 10, 64)
							if err != nil {
								fmt.Printf("can't force string to int64. row: %d field: %s", count-1, fieldNames[index])
								return nil, err
							}
							vals = append(vals, val)
						case bool:
							val, err := strconv.ParseBool(v)
							if err != nil {
								fmt.Printf("can't force string to bool. row: %d field: %s", count-1, fieldNames[index])
								return nil, err
							}
							if val == true {
								vals = append(vals, "true")
							} else {
								vals = append(vals, "false")
							}
						case time.Time:
							val, err := time.Parse(time.RFC3339, v)
							if err != nil {
								fmt.Printf("can't force string to time.Time. row: %d field: %s", count-1, fieldNames[index])
								return nil, err
							}
							vals = append(vals, val)
						default: // assign directly as string without conversion
							vals = append(vals, v)
						}

					}

				} else { // Append Values as normal (Default String)
					vals = append(vals, v)
				}

			}
			if init == nil {
				df.Append(vals...)
			} else {
				df.UpdateRow(count-1, vals...)
			}

		}
		count++
	}

	if count < 1 {
		return nil, errors.New("csv data contains no rows")
	}

	return dataframe.NewDataFrame(seriess...), nil
}

// JSONLoadOptions is likely to change.
type JSONLoadOptions struct {

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
// The first row determines which fields will be imported for subsequent rows.
// WARNING: The API may change in the future.
func LoadFromJSON(r io.ReadSeeker, options ...JSONLoadOptions) (*dataframe.DataFrame, error) {

	var init *dataframe.SeriesInit

	if len(options) > 0 {
		// Count how many rows we have in order to preallocate underlying slices
		if options[0].LargeDataSet {
			init = &dataframe.SeriesInit{}
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
						}
					}
				}
			}
		}
	}

	knownFields := map[string]interface{}{} // These fields are determined by the first row

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

			// The first row determines which fields we use
			knownFields = vals

			// Create a series for each field (of the appropriate data type)
			seriess := []dataframe.Series{}

			for name := range vals {

				// Check if we know what the datatype should be. Otherwise assume string
				if len(options) > 0 && len(options[0].DictateDataType) > 0 {

					typ, exists := options[0].DictateDataType[name]
					if !exists {
						seriess = append(seriess, dataframe.NewSeriesString(name, init))
						continue
					}

					switch typ.(type) {
					case float64:
						seriess = append(seriess, dataframe.NewSeriesFloat64(name, init))
					case int64:
						seriess = append(seriess, dataframe.NewSeriesInt64(name, init))
					case string:
						seriess = append(seriess, dataframe.NewSeriesString(name, init))
					case time.Time:
						seriess = append(seriess, dataframe.NewSeriesTime(name, init))
					default:
						seriess = append(seriess, dataframe.NewSeries(name, typ, init))
					}
				} else {
					seriess = append(seriess, dataframe.NewSeriesString(name, init))
				}

			}

			// Create the dataframe
			df = dataframe.NewDataFrame(seriess...)
			if init == nil {
				df.Append(make([]interface{}, len(df.Series))...)
			}

			// Store values of first row into dataframe
			insertVals := map[string]interface{}{}

			for name, val := range vals {

				// Store values
				if len(options) > 0 && len(options[0].DictateDataType) > 0 {

					// Check if a datatype is dictated
					typ, exists := options[0].DictateDataType[name]
					if !exists {
						// Store value as a string
						switch v := val.(type) {
						case string:
							insertVals[name] = v
						case json.Number:
							insertVals[name] = v.String()
						case bool:
							if v == true {
								insertVals[name] = "1"
							} else {
								insertVals[name] = "0"
							}
						}
					} else {
						err := dictateForce(row, insertVals, name, typ, val)
						if err != nil {
							return nil, err
						}
					}
				} else {
					// Store value as a string

					switch v := val.(type) {
					case string:
						insertVals[name] = v
					case json.Number:
						insertVals[name] = v.String()
					case bool:
						if v == true {
							insertVals[name] = "1"
						} else {
							insertVals[name] = "0"
						}
					}
				}

			}

			if len(insertVals) > 0 {
				if init == nil {
					df.Append(make([]interface{}, len(df.Series))...)
				}
				df.UpdateRow(row-1, insertVals)
			}

		} else {

			insertVals := map[string]interface{}{}

			for name, val := range vals {

				// Check if field is a known field
				_, exists := knownFields[name]
				if !exists {
					// unknown field
					if len(options) > 0 && options[0].ErrorOnUnknownFields {
						return nil, fmt.Errorf("unknown field encountered. row: %d field: %s", row-1, name)
					}
					continue
				}

				// Store values
				if len(options) > 0 && len(options[0].DictateDataType) > 0 {

					// Check if a datatype is dictated
					typ, exists := options[0].DictateDataType[name]
					if !exists {
						// Store value as a string
						switch v := val.(type) {
						case string:
							insertVals[name] = v
						case json.Number:
							insertVals[name] = v.String()
						case bool:
							if v == true {
								insertVals[name] = "1"
							} else {
								insertVals[name] = "0"
							}
						}
					} else {
						err := dictateForce(row, insertVals, name, typ, val)
						if err != nil {
							return nil, err
						}
					}
				} else {
					// Store value as a string

					switch v := val.(type) {
					case string:
						insertVals[name] = v
					case json.Number:
						insertVals[name] = v.String()
					case bool:
						if v == true {
							insertVals[name] = "1"
						} else {
							insertVals[name] = "0"
						}
					}
				}
			}

			if len(insertVals) > 0 {
				if init == nil {
					df.Append(make([]interface{}, len(df.Series))...)
				}
				df.UpdateRow(row-1, insertVals)
			}
		}
	}

	return df, nil
}
