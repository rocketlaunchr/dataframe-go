// Copyright 2019 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// Database is used to set the Database.
// Different databases have different syntax for placeholders etc.
type Database int

const (
	// PostgreSQL database
	PostgreSQL Database = 0
	// MySQL database
	MySQL Database = 1
)

// SQLLoadOptions is likely to change.
type SQLLoadOptions struct {

	// KnownRowCount is used to set the capacity of the underlying slices of the dataframe.
	// The maximum number of rows supported (on a 64-bit machine) is 9,223,372,036,854,775,807 (half of 64 bit range).
	// Preallocating memory can provide speed improvements. Benchmarks should be performed for your use-case.
	//
	// WARNING: Some databases may allow tables to contain more rows than the maximum supported.
	KnownRowCount *int

	// DictateDataType is used to inform LoadFromSQL what the true underlying data type is for a given field name.
	// The value for a given key must be of the data type of the data.
	// eg. For a string use "". For a int64 use int64(0). What is relevant is the data type and not the value itself.
	DictateDataType map[string]interface{}

	// Database is used to set the Database.
	Database Database
}

// LoadFromSQL will load data from a sql database.
func LoadFromSQL(ctx context.Context, stmt *sql.Stmt, options *SQLLoadOptions, args ...interface{}) (*dataframe.DataFrame, error) {

	var (
		init     *dataframe.SeriesInit
		database Database
		row      int
		df       *dataframe.DataFrame
	)

	if options != nil {

		if options.KnownRowCount != nil {
			init = &dataframe.SeriesInit{
				Size: *options.KnownRowCount,
			}
		}

		database = options.Database
		if database != PostgreSQL && database != MySQL {
			return nil, errors.New("invalid database")
		}
	}

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
	for _, ct := range cols { // ct is ColumnType
		name := ct.Name()
		typ := ct.DatabaseTypeName()

		// Check if data type is dictated and use if available
		if options != nil && len(options.DictateDataType) > 0 {
			if dtyp, exists := options.DictateDataType[name]; exists {

				switch T := dtyp.(type) {
				case float64:
					seriess = append(seriess, dataframe.NewSeriesFloat64(name, init))
				case int64, bool:
					seriess = append(seriess, dataframe.NewSeriesInt64(name, init))
				case string:
					seriess = append(seriess, dataframe.NewSeriesString(name, init))
				case time.Time:
					seriess = append(seriess, dataframe.NewSeriesTime(name, init))
				case Converter:
					seriess = append(seriess, dataframe.NewSeriesGeneric(name, T.ConcreteType, init))
				default:
					seriess = append(seriess, dataframe.NewSeriesGeneric(name, typ, init))
				}

				continue
			}
		}

		// Use typ if info is available
		switch typ {
		case "VARCHAR", "TEXT", "NVARCHAR", "MEDIUMTEXT", "LONGTEXT":
			seriess = append(seriess, dataframe.NewSeriesString(name, init))
		case "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DECIMAL", "NUMERIC":
			seriess = append(seriess, dataframe.NewSeriesFloat64(name, init))
		case "BOOL", "INT", "TINYINT", "INT2", "INT4", "INT8", "MEDIUMINT", "SMALLINT", "BIGINT":
			seriess = append(seriess, dataframe.NewSeriesInt64(name, init))
		case "DATETIME", "TIMESTAMP", "TIMESTAMPTZ":
			seriess = append(seriess, dataframe.NewSeriesTime(name, init))
		case "":
			// Assume string
			seriess = append(seriess, dataframe.NewSeriesString(name, init))
		default: // assume string if info is not available
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

		insertVals := map[string]interface{}{}
		for colID, elem := range rowData {

			colType := cols[colID].DatabaseTypeName()
			fieldName := cols[colID].Name()

			var val *string

			raw := elem.(*[]byte)
			if !(raw == nil || *raw == nil) {
				val = &[]string{string(*raw)}[0]
			}

			if val == nil {
				insertVals[fieldName] = nil
				continue
			}

			if options != nil && len(options.DictateDataType) > 0 {
				if dtyp, exists := options.DictateDataType[fieldName]; exists {

					switch T := dtyp.(type) {
					case float64:
						f, err := strconv.ParseFloat(*val, 64)
						if err != nil {
							return nil, fmt.Errorf("can't force string to float64. row: %d field: %s", row-1, fieldName)
						}
						insertVals[fieldName] = f
					case int64:
						n, err := strconv.ParseInt(*val, 10, 64)
						if err != nil {
							return nil, fmt.Errorf("can't force string to Int. row: %d field: %s", row-1, fieldName)
						}
						insertVals[fieldName] = n
					case string:
						insertVals[fieldName] = *val
					case bool:
						if *val == "true" || *val == "TRUE" || *val == "1" {
							insertVals[fieldName] = int64(1)
						} else if *val == "false" || *val == "FALSE" || *val == "0" {
							insertVals[fieldName] = int64(0)
						} else {
							return nil, fmt.Errorf("can't force string to bool. row: %d field: %s", row-1, fieldName)
						}
					case time.Time:
						layout := time.RFC3339 // Default for PostgreSQL
						if database == MySQL {
							layout = "2006-01-02 15:04:05"
						}

						t, err := time.Parse(layout, *val)
						if err != nil {
							// Assume unix timestamp
							sec, err := strconv.ParseInt(*val, 10, 64)
							if err != nil {
								return nil, fmt.Errorf("can't force string to time.Time (%s). row: %d field: %s", layout, row-1, fieldName)
							}
							t = time.Unix(sec, 0)
						}
						insertVals[fieldName] = t
					case Converter:
						cv, err := T.ConverterFunc(*val)
						if err != nil {
							return nil, fmt.Errorf("can't force string to generic data type. row: %d field: %s", row-1, fieldName)
						}
						insertVals[fieldName] = cv
					default:
						insertVals[fieldName] = *val
					}

					continue
				}
			}

			switch colType {
			case "VARCHAR", "TEXT", "NVARCHAR", "MEDIUMTEXT", "LONGTEXT":
				insertVals[fieldName] = *val
			case "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "FLOAT4", "FLOAT8":
				f, err := strconv.ParseFloat(*val, 64)
				if err != nil {
					return nil, fmt.Errorf("can't force string to float64. row: %d field: %s", row-1, fieldName)
				}
				insertVals[fieldName] = f
			case "INT", "TINYINT", "INT2", "INT4", "INT8", "MEDIUMINT", "SMALLINT", "BIGINT":
				n, err := strconv.ParseInt(*val, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("can't force string to Int. row: %d field: %s", row-1, fieldName)
				}
				insertVals[fieldName] = n
			case "BOOL":
				if *val == "true" || *val == "TRUE" || *val == "1" {
					insertVals[fieldName] = int64(1)
				} else if *val == "false" || *val == "FALSE" || *val == "0" {
					insertVals[fieldName] = int64(0)
				} else {
					return nil, fmt.Errorf("can't force string to bool. row: %d field: %s", row-1, fieldName)
				}
			case "DATETIME", "TIMESTAMP", "TIMESTAMPTZ":
				layout := time.RFC3339 // Default for PostgreSQL
				if database == MySQL {
					layout = "2006-01-02 15:04:05"
				}

				t, err := time.Parse(layout, *val)
				if err != nil {
					// Assume unix timestamp
					sec, err := strconv.ParseInt(*val, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("can't force string to time.Time (%s). row: %d field: %s", layout, row-1, fieldName)
					}
					t = time.Unix(sec, 0)
				}
				insertVals[fieldName] = t
			default:
				// Assume string
				insertVals[fieldName] = *val
			}
		}

		if init == nil {
			df.Append(make([]interface{}, len(df.Series))...)
		}
		df.UpdateRow(row-1, insertVals)

	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if df == nil {
		return nil, dataframe.ErrNoRows
	}

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
