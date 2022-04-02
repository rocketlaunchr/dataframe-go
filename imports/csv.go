// Copyright 2018-21 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

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

	jutils "github.com/juju/utils/v2"
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
	// eg. For a string use "". For an int64 use int64(0). What is relevant is the data type and not the value itself.
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
	// If the data type could not be detected, SeriesString is used.
	InferDataTypes bool

	// Headers must be set if the CSV file does not contain a header row. This must be nil if the CSV file contains a
	// header row.
	Headers []string
}

// LoadFromCSV will load data from a csv file.
func LoadFromCSV(ctx context.Context, r io.ReadSeeker, options ...CSVLoadOptions) (*dataframe.DataFrame, error) {

	var init *dataframe.SeriesInit

	// Check for bom characters in the beginning (that python seems to add).
	// See:
	// https://github.com/rocketlaunchr/dataframe-go/issues/62
	// https://github.com/golang/go/issues/33887
	// https://github.com/dimchansky/utfbom
	// https://github.com/spkg/bom/
	checkBOM := make([]byte, 3)
	readN, err := r.Read(checkBOM)
	if err != nil {
		return nil, err
	}
	if !(readN == 3 && checkBOM[0] == 0xef && checkBOM[1] == 0xbb && checkBOM[2] == 0xbf) {
		// bom not found so reset reader
		r.Seek(0, io.SeekStart)
	}

	var (
		comma            rune
		comment          rune
		trimLeadingSpace bool

		newR io.ReadSeeker = r
	)

	if len(options) > 0 {
		comma = options[0].Comma
		if comma == 0 {
			comma = ','
		}
		comment = options[0].Comment
		trimLeadingSpace = options[0].TrimLeadingSpace

		if len(options[0].Headers) > 0 {
			headers := strings.NewReader(strings.Join(options[0].Headers, string(comma)) + "\n")
			newR = jutils.NewMultiReaderSeeker(headers, r)
		}
	}

	cr := csv.NewReader(newR)
	cr.ReuseRecord = true
	if len(options) > 0 {
		cr.Comma = comma
		cr.Comment = comment
		cr.TrimLeadingSpace = trimLeadingSpace

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
						newR.Seek(0, io.SeekStart)
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

			// Create the series
			for _, name := range rec {

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

					continue
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

			// Create the dataframe
			df = dataframe.NewDataFrame(seriess...)
		} else {

			insertVals := []interface{}{}
			for idx, v := range rec {

				// Check if v represents a nil value
				if len(options) > 0 && options[0].NilValue != nil {
					if v == *options[0].NilValue {
						insertVals = append(insertVals, nil)
						continue
					}
				}

				// Check if the datatype is dictated
				if len(options) > 0 && len(options[0].DictateDataType) > 0 {

					name := df.Names(dataframe.DontLock)[idx]

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
							return nil, fmt.Errorf("can't force string: %s to bool. row: %d field: %s", v, row-1, name)
						}
					case int64:
						i, err := strconv.ParseInt(v, 10, 64)
						if err != nil {
							return nil, fmt.Errorf("can't force string: %s to int64. row: %d field: %s", v, row-1, name)
						}
						insertVals = append(insertVals, i)
					case float64:
						f, err := strconv.ParseFloat(v, 64)
						if err != nil {
							return nil, fmt.Errorf("can't force string: %s to float64. row: %d field: %s", v, row-1, name)
						}
						insertVals = append(insertVals, f)
					case time.Time:
						t, err := time.Parse(time.RFC3339, v)
						if err != nil {
							// Assume unix timestamp
							sec, err := strconv.ParseInt(v, 10, 64)
							if err != nil {
								return nil, fmt.Errorf("can't force string: %s to time.Time (%s). row: %d field: %s", v, time.RFC3339, row-1, name)
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
							return nil, fmt.Errorf("can't force string: %s to generic data type. row: %d field: %s", v, row-1, name)
						}
						insertVals = append(insertVals, cv)
					default:
						insertVals = append(insertVals, v)
					}

					continue
				}

			INFER2:

				// Datatype is either inferred or assumed to be a string
				insertVals = append(insertVals, v)
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
