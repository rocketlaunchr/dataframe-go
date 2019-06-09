// Copyright 2019 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"context"
	"database/sql"
	"errors"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// SQLLoadOptions is likely to change.
type SQLLoadOptions struct {

	// KnownRowCount is used to set the capacity of the underlying slices of the dataframe.
	// The maximum number of rows supported (on a 64-bit machine) is 9,223,372,036,854,775,807 (half of 64 bit range).
	// Preallocating memory can provide speed improvements. Benchmarks should be performed for your use-case.
	//
	// WARNING: Some databases may allow for full 64 bit range.
	KnownRowCount *int

	// DictateDataType is used to inform LoadFromSQL what the true underlying data type is for a given field name.
	// The value for a given key must be of the data type of the data.
	// eg. For a string use "". For a int64 use int64(0). What is relevant is the data type and not the value itself.
	DictateDataType map[string]interface{}
}

// LoadFromSQL will load data from a sql database.
// WARNING: The API may change in the future.
func LoadFromSQL(ctx context.Context, stmt *sql.Stmt, options *SQLLoadOptions, args ...interface{}) (*dataframe.DataFrame, error) {

	var init *dataframe.SeriesInit

	if options != nil {

		if options.KnownRowCount != nil {
			init = &dataframe.SeriesInit{
				Size: *options.KnownRowCount,
			}
		}

	}

	var row int
	var df *dataframe.DataFrame

	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, _ := rows.ColumnTypes()
	totalColumns := len(cols)

	if totalColumns <= 0 {
		return nil, errors.New("no series found")
	}

	// Create the dataframe
	seriess := []dataframe.Series{}
	for i, ct := range cols { // ct is ColumnType
		name := ct.Name()
		typ := ct.DatabaseTypeName()

		// Check if data type is dictated
		if options != nil && len(options.DictateDataType) > 0 {
			if typ, exists := options.DictateDataType[name]; exists {
				// TODO:

				_ = typ
				_ = i

				continue
			}
		}

		// Use typ if info is available
		switch typ {
		case "VARCHAR", "TEXT", "NVARCHAR":
			seriess = append(seriess, dataframe.NewSeriesString(name, init))
		case "DECIMAL": // float64??? float32???
			seriess = append(seriess, dataframe.NewSeriesFloat64(name, init))
		case "BOOL", "INT", "BIGINT":
			seriess = append(seriess, dataframe.NewSeriesInt64(name, init))
		case "":
			// Assume string
			seriess = append(seriess, dataframe.NewSeriesString(name, init))
		default:
			seriess = append(seriess, dataframe.NewSeriesString(name, init))
		}
	}
	df = dataframe.NewDataFrame(seriess...)

	for rows.Next() {
		row++

		rowData := make([]interface{}, totalColumns)
		for i := range rowData {
			rowData[i] = &[]byte{}
		}

		if err := rows.Scan(rowData...); err != nil {
			return nil, err
		}

	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if df == nil {
		return nil, dataframe.ErrNoRows
	} else {
		// Remove unused preallocated rows from dataframe
		if init != nil {
			excess := init.Size - df.NRows()
			for {
				if excess <= 0 {
					break
				}
				df.Remove(df.NRows() - 1) // remove current last row
				excess--
			}
		}
		return df, nil
	}

	panic("TODO: LoadFromSQL not implemented")

}
