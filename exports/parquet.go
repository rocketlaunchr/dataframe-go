// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package exports

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/davecgh/go-spew/spew"
	dynamicstruct "github.com/ompluscator/dynamic-struct"
	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// ParquetExportOptions contains options for ExportToParquet function.
type ParquetExportOptions struct {

	// Range is used to export a subset of rows from the dataframe.
	Range dataframe.Range

	PageSize        *int64
	CompressionType parquet.CompressionCodec
	Offset          *int64
}

// ExportToParquet exports a Dataframe to a CSV file.
func ExportToParquet(ctx context.Context, outputFilePath string, df *dataframe.DataFrame, options ...ParquetExportOptions) error {

	df.Lock()
	defer df.Unlock()

	var (
		r dataframe.Range
	)

	if len(options) > 0 {
		r = options[0].Range
	}

	// Create Schema
	dataSchema := dynamicstruct.NewStruct()
	for _, aSeries := range df.Series {
		name := strings.ToLower(aSeries.Name())
		title := strings.Join([]string{strings.ToUpper(string(name[0])), name[1:]}, "")

		fmt.Println("name:", name, "\ntitle:", title)

		switch aSeries.(type) {
		case *dataframe.SeriesFloat64:
			tag := fmt.Sprintf(`parquet:"name=%s, type=DOUBLE"`, name)

			dataSchema.AddField(title, 0.0, tag)

		case *dataframe.SeriesInt64:
			tag := fmt.Sprintf(`parquet:"name=%s, type=INT64"`, name)

			dataSchema.AddField(title, int64(0), tag)

		case *dataframe.SeriesTime:
			tag := fmt.Sprintf(`parquet:"name=%s, type=TIME_MILLIS"`, name)

			dataSchema.AddField(title, nil, tag)

		case *dataframe.SeriesString:
			tag := fmt.Sprintf(`parquet:"name=%s, type=UTF8, encoding=PLAIN_DICTIONARY"`, name)

			dataSchema.AddField(title, "", tag)

		default:
			tag := fmt.Sprintf(`parquet:"name=name, type=UTF8, encoding=PLAIN_DICTIONARY"`)
			dataSchema.AddField(title, "", tag)
		}

	}

	spew.Dump(dataSchema.Build().New())

	fw, err := local.NewLocalFileWriter(outputFilePath)
	if err != nil {
		return err
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, dataSchema.Build().New(), 4)
	if err != nil {
		return err
	}

	pw.CompressionType = options[0].CompressionType
	if options[0].Offset != nil {
		pw.Offset = *options[0].Offset
	}
	// if options[0].PageSize != nil {
	// 	pw.PageSize = *options[0].PageSize
	// }

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

			// Next issue: How to add values into a struct
			rec := dataSchema.Build().New()
			for _, aSeries := range df.Series {

				sName := strings.ToLower(aSeries.Name())

				v := reflect.ValueOf(rec).Elem().FieldByName(sName)

				if v.IsValid() {
					val := aSeries.Value(row)

					v.Set(reflect.ValueOf(val))
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
