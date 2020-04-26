// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package exports

import (
	"context"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// ParquetExportOptions contains options for ExportToParquet function.
type ParquetExportOptions struct {

	// NullString is used to set what nil values should be encoded to.
	// Common options are NULL, \N, NaN, NA.
	NullString *string

	// Range is used to export a subset of rows from the dataframe.
	Range dataframe.Range

	Schema []string

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
		null   string = "null" // default is null
		schema []string

		pageSize        int64
		rowGroupSize    int64
		compressionType parquet.CompressionCodec
		offset          int64
	)

	if len(options) > 0 {

		r = options[0].Range
		schema = options[0].Schema

		if options[0].NullString != nil {
			null = *options[0].NullString
		}
		if options[0].Offset != nil {
			offset = *options[0].Offset
		}
		if options[0].RowGroupSize != nil {
			rowGroupSize = *options[0].RowGroupSize
		}
		if options[0].PageSize != nil {
			pageSize = *options[0].PageSize
		}
		if options[0].CompressionType != nil {
			compressionType = options[0].CompressionType
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

	nRows := df.NRows(dataframe.DontLock)
	if nRows > 0 {
		pw.Offset = offset
		pw.RowGroupSize = rowGroupSize
		pw.PageSize = pageSize
		pw.CompressionType = compressionType

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
					rec = append(rec, &null)
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
