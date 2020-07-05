// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
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
	// It will set the capacity of the underlying slices of the Dataframe by performing a basic parse
	// of the full dataset before processing the data fully.
	// Preallocating memory can provide speed improvements. Benchmarks should be performed for your use-case.
	LargeDataSet bool

	// DictateDataType is used to inform LoadFromCSV what the true underlying data type is for a given field name.
	// The key must be the case-sensitive field name.
	// The value for a given key must be of the data type of the data.
	// eg. For a string use "". For a int64 use int64(0). What is relevant is the data type and not the value itself.
	//
	// NOTE: A custom Series must implement NewSerieser interface and be able to interpret strings to work.
	DictateDataType map[string]interface{}

	// NilValue allows you to set what string value in the CSV file should be interpreted as a nil value for
	// the purposes of insertion.
	//
	// Common values are: NULL, \N, NaN, NA
	NilValue *string

	// InferDataTypes can be set to true if the underlying data type should be automatically detected.
	// Using DictateDataType is the recommended approach (especially for large datasets or memory constrained systems).
	// DictateDataType always takes precedence when determining the type.
	// If the data type could not be detected, NewSeriesString is used.
	InferDataTypes bool

	// Merge multiple columns into one, values are joined by spaces.
	// eg. Join date and time columns into one datetime column.
	MergeColumns map[string][]string

	// Format that should be used to parse time string. Default uses time.RFC3339.
	TimeFormat string
}

// LoadFromCSV will load data from a csv file.
func LoadFromCSV(ctx context.Context, r io.ReadSeeker, options ...CSVLoadOptions) (*dataframe.DataFrame, error) {

	var init *dataframe.SeriesInit

	cr := csv.NewReader(r)
	cr.ReuseRecord = true
	if len(options) > 0 {
		cr.Comma = options[0].Comma
		if cr.Comma == 0 {
			cr.Comma = ','
		}
		cr.Comment = options[0].Comment
		cr.TrimLeadingSpace = options[0].TrimLeadingSpace

		// Count how many rows we have in order to preallocate underlying slices
		if options[0].LargeDataSet {
			init = &dataframe.SeriesInit{}
			for {
				if err := ctx.Err(); err != nil {
					return nil, err
				}

				_, err := cr.Read()
				if err != nil {
					if err == io.EOF {
						r.Seek(0, io.SeekStart)
						break
					}
					return nil, err
				}
				init.Capacity++
			}
			if init.Capacity > 0 {
				init.Capacity-- // Remove the space allocated for the "heading"
			}
		}
	}

	var row int
	var df *dataframe.DataFrame

	// key: new column name
	// value: indices of the columns that are going to be merged
	mergingColumn := map[string][]int{}
	// this is used to guarantee the order of the merged columns
	var mergedColumns []string

	// check for custom time format
	timeFormat := time.RFC3339
	if len(options) > 0 && options[0].TimeFormat != "" {
		timeFormat = options[0].TimeFormat
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		rec, err := cr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if row == 0 {
			// First row contains headings

			seriess := []dataframe.Series{}

			// check to see if the column is going to be merged
			isToBeMerged := func(clmIndex int, name string) bool {
				if clmIndex < 0 {
					return false
				}
				if len(options) > 0 && len(options[0].MergeColumns) > 0 {
					for merged, columnsToMerge := range options[0].MergeColumns {
						if _, found := mergingColumn[merged]; !found {
							mergingColumn[merged] = make([]int, len(columnsToMerge))
							mergedColumns = append(mergedColumns, merged)
							for i := range mergingColumn[merged] {
								// assign default value for later verification
								mergingColumn[merged][i] = -1
							}
						}
						for orderIndex, column := range columnsToMerge {
							if column == name {
								// insert the column index in the order of configuration
								mergingColumn[merged][orderIndex] = clmIndex
								return true
							}
						}
					}
				}
				return false
			}

			prepareSeries := func(index int, name string) {

				if isToBeMerged(index, name) {
					return
				}

				// Check if the datatype is dictated
				if len(options) > 0 && len(options[0].DictateDataType) > 0 {
					typ, exists := options[0].DictateDataType[name]
					if !exists {
						goto INFER1
					}

					switch T := typ.(type) {
					case float64:
						seriess = append(seriess, dataframe.NewSeriesFloat64(name, init))
					case int64, bool:
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

					return
				}

			INFER1:

				if len(options) > 0 && options[0].InferDataTypes {
					var knownSize *int
					if init != nil {
						knownSize = &init.Capacity
					}
					is := newInferSeries(name, knownSize)
					seriess = append(seriess, is)
				} else {
					// Default assumption is string
					seriess = append(seriess, dataframe.NewSeriesString(name, init))
				}
			}

			// Create the series
			for index, name := range rec {
				prepareSeries(index, name)
			}

			for _, column := range mergedColumns {
				// Create the merged columns
				prepareSeries(-1, column)
			}

			for mergedColumn, indices := range mergingColumn {
				for _, index := range indices {
					if index < 0 {
						return nil, fmt.Errorf("some of the columns don't exist for merging into column %s", mergedColumn)
					}
				}
			}

			// Create the dataframe
			df = dataframe.NewDataFrame(seriess...)
		} else {

			insertVals := []interface{}{}
			// key: merged column name
			// value: merged values
			stringsToMerge := map[string][]string{}

			isToBeMerged := func(index int, value string) bool {
				itIs := false
				for _, merged := range mergedColumns {
					if _, found := stringsToMerge[merged]; !found {
						stringsToMerge[merged] = make([]string, len(mergingColumn[merged]))
					}
					for order, i := range mergingColumn[merged] {
						if i == index {
							// insert the value in the order of configuration
							stringsToMerge[merged][order] = value
							itIs = true
						}
					}
				}
				return itIs
			}

			columnIndex := -1
			processValue := func(index int, v string) error {
				if isToBeMerged(index, v) {
					return nil
				}
				columnIndex++

				// Check if v represents a nil value
				if len(options) > 0 && options[0].NilValue != nil {
					if v == *options[0].NilValue {
						insertVals = append(insertVals, nil)
						return nil
					}
				}

				// Check if the datatype is dictated
				if len(options) > 0 && len(options[0].DictateDataType) > 0 {

					name := df.Names(dataframe.DontLock)[columnIndex]

					// Check if a datatype is dictated
					typ, exists := options[0].DictateDataType[name]
					if !exists {
						goto INFER2
					}

					switch T := typ.(type) {
					case string:
						insertVals = append(insertVals, v)
					case bool:
						if v == "TRUE" || v == "true" || v == "True" || v == "1" {
							insertVals = append(insertVals, int64(1))
						} else if v == "FALSE" || v == "false" || v == "False" || v == "0" {
							insertVals = append(insertVals, int64(0))
						} else {
							return fmt.Errorf("can't force string: %s to bool. row: %d field: %s", v, row-1, name)
						}
					case int64:
						i, err := strconv.ParseInt(v, 10, 64)
						if err != nil {
							return fmt.Errorf("can't force string: %s to int64. row: %d field: %s", v, row-1, name)
						}
						insertVals = append(insertVals, i)
					case float64:
						f, err := strconv.ParseFloat(v, 64)
						if err != nil {
							return fmt.Errorf("can't force string: %s to float64. row: %d field: %s", v, row-1, name)
						}
						insertVals = append(insertVals, f)
					case time.Time:
						t, err := time.Parse(timeFormat, v)
						if err != nil {
							// Assume unix timestamp
							sec, err := strconv.ParseInt(v, 10, 64)
							if err != nil {
								return fmt.Errorf("can't force string: %s to time.Time (%s). row: %d field: %s", v, time.RFC3339, row-1, name)
							}
							insertVals = append(insertVals, time.Unix(sec, 0))
						} else {
							insertVals = append(insertVals, t)
						}
					case dataframe.NewSerieser:
						insertVals = append(insertVals, v)
					case Converter:
						cv, err := T.ConverterFunc(v)
						if err != nil {
							return fmt.Errorf("can't force string: %s to generic data type. row: %d field: %s", v, row-1, name)
						}
						insertVals = append(insertVals, cv)
					default:
						insertVals = append(insertVals, v)
					}

					return nil
				}

			INFER2:

				// Datatype is either inferred or assumed to be a string
				insertVals = append(insertVals, v)
				return nil
			}

			for idx, v := range rec {
				err := processValue(idx, v)
				if err != nil {
					return nil, err
				}
			}

			// append merged column values
			for _, column := range mergedColumns {
				err := processValue(-1, strings.Join(stringsToMerge[column], " "))
				if err != nil {
					return nil, err
				}
			}

			df.Append(&dataframe.DontLock, insertVals...)
		}
		row++
	}

	if df == nil {
		return nil, dataframe.ErrNoRows
	}

	// Convert inferred series to actual series
	if len(options) > 0 && options[0].InferDataTypes {
		for idx := len(df.Series) - 1; idx >= 0; idx-- {
			s := df.Series[idx]

			is, ok := s.(*inferSeries)
			if !ok {
				continue
			}

			ns, _ := is.inferred()
			df.Series[idx] = ns
		}
	}

	return df, nil
}
