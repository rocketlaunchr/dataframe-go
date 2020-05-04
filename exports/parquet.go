// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package exports

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	dynamicstruct "github.com/ompluscator/dynamic-struct"
	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// ParquetExportOptions contains options for ExportToParquet function.
type ParquetExportOptions struct {

	// Range is used to export a subset of rows from the dataframe.
	Range dataframe.Range

	// PageSize defaults to 8K if not set set.
	//
	// See: https://godoc.org/github.com/xitongsys/parquet-go/writer#ParquetWriter
	PageSize *int64

	// CompressionType defaults to CompressionCodec_SNAPPY if not set.
	//
	// See: https://godoc.org/github.com/xitongsys/parquet-go/writer#ParquetWriter
	CompressionType *parquet.CompressionCodec

	// Offset defaults to 4 if not set.
	//
	// See: https://godoc.org/github.com/xitongsys/parquet-go/writer#ParquetWriter
	Offset *int64
}

// ExportToParquet exports a Dataframe as a Parquet file.
func ExportToParquet(ctx context.Context, w io.Writer, df *dataframe.DataFrame, options ...ParquetExportOptions) error {

	df.Lock()
	defer df.Unlock()

	var (
		r               dataframe.Range
		compressionType *parquet.CompressionCodec
		offset          *int64
		pageSize        *int64
	)

	//include the createdBy to avoid

	if len(options) > 0 {
		r = options[0].Range
		compressionType = options[0].CompressionType
		pageSize = options[0].PageSize
		offset = options[0].Offset
	}

	// Create Schema
	dataSchema := dynamicstruct.NewStruct()
	for _, aSeries := range df.Series {
		fieldName := strings.Title(strings.ToLower(aSeries.Name()))

		switch aSeries.(type) {
		case *dataframe.SeriesFloat64:
			tag := fmt.Sprintf(`parquet:"name=%s, type=DOUBLE"`, aSeries.Name())
			dataSchema.AddField(fieldName, (float64)(0.0), tag)
		case *dataframe.SeriesInt64:
			tag := fmt.Sprintf(`parquet:"name=%s, type=INT64"`, aSeries.Name())
			dataSchema.AddField(fieldName, (int64)(0), tag)
		case *dataframe.SeriesTime:
			tag := fmt.Sprintf(`parquet:"name=%s, type=TIME_MICROS"`, aSeries.Name())
			dataSchema.AddField(fieldName, (*int64)(nil), tag)
		case *dataframe.SeriesString:
			tag := fmt.Sprintf(`parquet:"name=%s, type=UTF8, encoding=PLAIN_DICTIONARY"`, aSeries.Name())
			dataSchema.AddField(fieldName, (string)(""), tag)
		default:
			tag := fmt.Sprintf(`parquet:"name=%s, type=UTF8, encoding=PLAIN_DICTIONARY"`, aSeries.Name())
			dataSchema.AddField(fieldName, (string)(""), tag)
		}

	}

	schemaStruct := dataSchema.Build()

	spew.Dump(schemaStruct.New())

	fw := writerfile.NewWriterFile(w)
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, schemaStruct.New(), 4)
	if err != nil {
		return err
	}

	if compressionType != nil {
		pw.CompressionType = *compressionType
	}

	if offset != nil {
		pw.Offset = *offset
	}

	if pageSize != nil {
		pw.PageSize = *pageSize
	}

	nRows := df.NRows(dataframe.DontLock)
	if nRows > 0 {

		s, e, err := r.Limits(nRows)
		if err != nil {
			return err
		}

		for row := s; row <= e; row++ {
			if err := ctx.Err(); err != nil {
				return err
			}

			rec := schemaStruct.New()
			for _, aSeries := range df.Series {
				fieldName := strings.Title(strings.ToLower(aSeries.Name()))

				v := reflect.ValueOf(rec).Elem().FieldByName(fieldName)
				if v.IsValid() {
					val := aSeries.Value(row) // returns an interface{}
					if val != nil {
						switch vl := val.(type) {
						case float64:
							v.Set(reflect.ValueOf(vl))
						case int64:
							v.Set(reflect.ValueOf(vl))
						case string:
							v.Set(reflect.ValueOf(vl))
						case time.Time:
							t := vl.UnixNano() / int64(time.Microsecond)
							v.Set(reflect.ValueOf(&t))
						default: // interface{}
							str := aSeries.ValueString(row)
							v.Set(reflect.ValueOf(str))
						}
					}
				}
			}
			if err := pw.Write(rec); err != nil {
				return err
			}
		}
		if err := pw.WriteStop(); err != nil {
			return err
		}
	}

	return nil
}
