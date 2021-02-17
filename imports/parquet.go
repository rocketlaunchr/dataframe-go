// Copyright 2018-21 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// ParquetLoadOptions is likely to change.
type ParquetLoadOptions struct {
}

// LoadFromParquet will load data from a parquet file.
//
// NOTE: This function is experimental and the implementation is likely to change.
//
// Example (gist):
//
//  import	"github.com/xitongsys/parquet-go-source/local"
//  import	"github.com/rocketlaunchr/dataframe-go/imports"
//
//  func main() {
//  	fr, _ := local.NewLocalFileReader("file.parquet")
//  	defer fr.Close()
//
//  	df, _ := imports.LoadFromParquet(ctx, fr)
//  }
//
func LoadFromParquet(ctx context.Context, src source.ParquetFile, opts ...ParquetLoadOptions) (*dataframe.DataFrame, error) {
	pr, err := reader.NewParquetReader(src, nil, int64(runtime.NumCPU()))
	if err != nil {
		return nil, err
	}
	defer pr.ReadStop()

	nRows := int(pr.GetNumRows())
	init := &dataframe.SeriesInit{Capacity: nRows}

	// Determine number of columns & create series of correct type
	if pr.ObjType == nil {
		if pr.ObjType, err = pr.SchemaHandler.GetType(pr.SchemaHandler.GetRootInName()); err != nil {
			return nil, err
		}
	}

	goRootName := pr.SchemaHandler.SchemaElements[0].Name
	actualRootName := pr.SchemaHandler.InPathToExPath[goRootName]

	// Map Go Field name to time field
	goTimeFields := map[string]parquet.ConvertedType{}
	for _, se := range pr.SchemaHandler.SchemaElements {
		if se.ConvertedType != nil {
			switch *se.ConvertedType {
			case parquet.ConvertedType_TIME_MILLIS, parquet.ConvertedType_TIME_MICROS, parquet.ConvertedType_TIMESTAMP_MILLIS, parquet.ConvertedType_TIMESTAMP_MICROS:
				goTimeFields[se.Name] = *se.ConvertedType
			}
		}
	}

	// Map Go Field name to actual field name
	goFieldNameToActual := map[string]string{}

	for _, goName := range pr.SchemaHandler.ValueColumns {
		goFieldNameToActual[strings.TrimPrefix(goName, goRootName+".")] = strings.TrimPrefix(pr.SchemaHandler.InPathToExPath[goName], actualRootName+".")
	}

	// Create Series and DataFrame (Parquet file returns the data type)
	seriess := []dataframe.Series{}

	for i := 0; i < pr.ObjType.NumField(); i++ {
		field := pr.ObjType.Field(i)
		goName := field.Name
		actualName := goFieldNameToActual[goName]

		// Check if goName is a time series
		_, ok := goTimeFields[goName]
		if ok {
			seriess = append(seriess, dataframe.NewSeriesTime(actualName, init))
		} else {
			kind := field.Type.Kind()
			if kind == reflect.Ptr {
				kind = field.Type.Elem().Kind()
			}

			switch kind {
			case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				seriess = append(seriess, dataframe.NewSeriesInt64(actualName, init))
			case reflect.Float32, reflect.Float64:
				seriess = append(seriess, dataframe.NewSeriesFloat64(actualName, init))
			case reflect.String:
				seriess = append(seriess, dataframe.NewSeriesString(actualName, init))
			default:
				panic("unrecognized data type for column: " + actualName)
			}
		}
	}

	// Create the dataframe
	df := dataframe.NewDataFrame(seriess...)

	// Load data to Series
	vs := reflect.MakeSlice(reflect.SliceOf(pr.ObjType), 1, 1)
	res := reflect.New(vs.Type())
	res.Elem().Set(vs)

	for i := 0; i < nRows; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if err = pr.Read(res.Interface()); err != nil {
			return nil, err
		}
		row := res.Elem().Index(0)
		insertVals := map[string]interface{}{}

		for j := 0; j < row.NumField(); j++ { // iterate over fields in row
			goName := pr.ObjType.Field(j).Name
			name := goFieldNameToActual[goName]
			field := row.Field(j)
			val := field.Interface()

			if val == nil {
				insertVals[name] = nil
				continue
			}

			// Check if data is meant to be a time
			if timeType, ok := goTimeFields[goName]; ok {
				if timeType == parquet.ConvertedType_TIME_MILLIS {
					switch v := val.(type) {
					case *int64:
						if v == nil {
							insertVals[name] = nil
						} else {
							insertVals[name] = time.Unix(0, *v*1000000).In(time.UTC)
						}
					case int64:
						insertVals[name] = time.Unix(0, v*1000000).In(time.UTC)
					}
				} else if timeType == parquet.ConvertedType_TIME_MICROS {
					switch v := val.(type) {
					case *int64:
						if v == nil {
							insertVals[name] = nil
						} else {
							insertVals[name] = time.Unix(0, *v*1000).In(time.UTC)
						}
					case int64:
						insertVals[name] = time.Unix(0, v*1000).In(time.UTC)
					}
				} else {

				}
			} else {
				switch v := val.(type) {
				case *string:
					if v == nil {
						insertVals[name] = nil
					} else {
						insertVals[name] = *v
					}
				case string:
					insertVals[name] = v
				case *float32:
					if v == nil {
						insertVals[name] = nil
					} else {
						insertVals[name] = float64(*v)
					}
				case float32:
					insertVals[name] = float64(v)
				case *float64:
					if v == nil {
						insertVals[name] = nil
					} else {
						insertVals[name] = *v
					}
				case float64:
					insertVals[name] = v
				case *uint8:
					if v == nil {
						insertVals[name] = nil
					} else {
						insertVals[name] = int64(*v)
					}
				case uint8:
					insertVals[name] = int64(v)
				case *uint16:
					if v == nil {
						insertVals[name] = nil
					} else {
						insertVals[name] = int64(*v)
					}
				case uint16:
					insertVals[name] = int64(v)
				case *uint32:
					if v == nil {
						insertVals[name] = nil
					} else {
						insertVals[name] = int64(*v)
					}
				case uint32:
					insertVals[name] = int64(v)
				case *uint64:
					if v == nil {
						insertVals[name] = nil
					} else {
						insertVals[name] = *v
					}
				case uint64:
					insertVals[name] = v
				case *int8:
					if v == nil {
						insertVals[name] = nil
					} else {
						insertVals[name] = int64(*v)
					}
				case int8:
					insertVals[name] = int64(v)
				case *int16:
					if v == nil {
						insertVals[name] = nil
					} else {
						insertVals[name] = int64(*v)
					}
				case int16:
					insertVals[name] = int64(v)
				case *int32:
					if v == nil {
						insertVals[name] = nil
					} else {
						insertVals[name] = int64(*v)
					}
				case int32:
					insertVals[name] = int64(v)
				case *int64:
					if v == nil {
						insertVals[name] = nil
					} else {
						insertVals[name] = *v
					}
				case int64:
					insertVals[name] = v
				default:
					panic("unrecognized data type for column: " + name)
				}
			}
		}

		df.Append(&dataframe.DontLock, insertVals)
	}

	return df, nil
}
