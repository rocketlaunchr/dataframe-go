// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package exports

import (
	"context"
	"fmt"

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
	RowGroupSize    *int64
	CompressionType parquet.CompressionCodec
	Offset          *int64
}

// ExportToParquet exports a Dataframe to a CSV file.
func ExportToParquet(ctx context.Context, outputFilePath string, df *dataframe.DataFrame, options ...ParquetExportOptions) error {

	df.Lock()
	defer df.Unlock()

	var (
		r      dataframe.Range
		schema []string
	)

	if len(options) > 0 {

		r = options[0].Range

	}

	// Create Schema
	for _, aSeries := range df.Series {
		name := aSeries.Name()

		switch aSeries.(type) {
		case *dataframe.SeriesFloat64:
			schema = append(schema, fmt.Sprintf("name=%s, type=FLOAT", name))
		case *dataframe.SeriesInt64:
			schema = append(schema, fmt.Sprintf("name=%s, type=INT64", name))
		case *dataframe.SeriesTime:
			schema = append(schema, fmt.Sprintf("name=%s, type=TIME_MILLIS", name))
		case *dataframe.SeriesString:
			schema = append(schema, fmt.Sprintf("name=%s, type=UTF8, encoding=PLAIN_DICTIONARY", name))
		default:
			schema = append(schema, fmt.Sprintf("name=%s, type=UTF8, encoding=PLAIN_DICTIONARY", name))
		}

	}

	fw, err := local.NewLocalFileWriter(outputFilePath)
	if err != nil {
		return err
	}
	defer fw.Close()

	pw, err := writer.NewCSVWriter(schema, fw, 4)
	if err != nil {
		return err
	}

	// // pw.CompressionType = options[0].CompressionType
	// if options[0].Offset != nil {
	// 	pw.Offset = *options[0].Offset
	// }
	// if options[0].RowGroupSize != nil {
	// 	pw.RowGroupSize = *options[0].RowGroupSize
	// }
	// if options[0].PageSize != nil {
	// 	pw.PageSize = *options[0].PageSize
	// }

	nRows := df.NRows(dataframe.DontLock)
	if nRows > 0 {
		// pw.Offset = offset
		// pw.RowGroupSize = rowGroupSize
		// pw.PageSize = pageSize
		// pw.CompressionType = compressionType

		s, e, err := r.Limits(nRows)
		if err != nil {
			return err
		}

		for row := s; row <= e; row++ {

			if err := ctx.Err(); err != nil {
				return err
			}

			rec := []*string{}
			for _, aSeries := range df.Series {

				val := aSeries.Value(row)
				if val == nil {
					rec = append(rec, nil)
				} else {
					v := aSeries.ValueString(row, dataframe.DontLock)
					rec = append(rec, &v)
				}
			}

			if err := pw.WriteString(rec); err != nil {
				return err
			}

		}
		if err := pw.WriteStop(); err != nil {
			return err
		}

	}

	return nil
}
