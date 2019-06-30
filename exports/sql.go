package exports

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/davecgh/go-spew/spew"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

type execContexter interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// SQLExportOptions contains options for ExportToSQL function.
type SQLExportOptions struct {

	// NullString is used to set what nil values should be encoded to.
	// Common options are NULL, \N, NaN, NA.
	NullString *string

	// Range is used to export a subset of rows from the dataframe.
	Range dataframe.Range

	// PrimaryKey is used if you want to generate custom values for the primary key
	PrimaryKey *PrimaryKey

	// BatchSize is used to insert data in batches.
	// It is recommended a transaction is used so if 1 batch-insert fails, then all
	// successfully inserted data can be rolled back.
	// If set, it must not be 0.
	BatchSize *int

	// SeriesToColumn is used to export a series (key of map) to a column in the table.
	// If the key does not exist, the series name is used by default.
	// If the column value is nil, the series is ignored for the purposes of exporting.
	SeriesToColumn map[string]*string
}

// PrimaryKey is used to generate custom values for the primary key
type PrimaryKey struct {

	// PrimaryKey is the column name of primary key
	PrimaryKey string

	// Value is a function that generates a primary key id given the row number
	// and number of rows in the Dataframe.
	// For auto-incrementing primary keys, nil can be returned.
	Value func(row int, n int) *string
}

// ExportToSQL exports a dataframe to a SQL Database.
func ExportToSQL(ctx context.Context, db execContexter, df *dataframe.DataFrame, tableName string, options ...SQLExportOptions) error {

	df.Lock()
	defer df.Unlock()

	var (
		null      *string
		r         dataframe.Range
		pk        *PrimaryKey
		batchSize *int
	)

	if tableName == "" {
		return errors.New("invalid tableName")
	}

	seriesToColumn := map[string]*string{}

	if len(options) > 0 {
		null = options[0].NullString
		r = options[0].Range
		pk = options[0].PrimaryKey
		if pk != nil && (*pk).PrimaryKey == "" {
			return errors.New("invalid primary key name")
		}
		batchSize = options[0].BatchSize
		if batchSize != nil && *batchSize == 0 {
			return errors.New("invalid BatchSize")
		}
		if options[0].SeriesToColumn != nil {
			seriesToColumn = options[0].SeriesToColumn
		}
	}

	nRows := df.NRows(dataframe.DontLock)
	if nRows == 0 {
		return nil
	}

	start, end, err := r.Limits(nRows)
	if err != nil {
		return err
	}

	iterator := df.Values(dataframe.ValuesOptions{InitialRow: start, Step: 1, DontReadLock: true})

	var batchData []map[string]*string

	batchCount := 0
	for {
		// context has been canceled
		if err := ctx.Err(); err != nil {
			return err
		}

		row, vals := iterator()
		if row == nil || *row > end {
			break
		}

		batchCount = batchCount + 1

		rowData := map[string]*string{}

		// Insert primary key
		if pk != nil {
			if pk.Value != nil {
				rowData[pk.PrimaryKey] = pk.Value(*row, nRows)
			} else {
				rowData[pk.PrimaryKey] = nil
			}
		}

		for k, val := range vals {
			switch colIdx := k.(type) {
			case int:
				seriesName := df.Series[colIdx].Name()

				colName, exists := seriesToColumn[seriesName]
				if exists && colName == nil {
					// Ignore column
					continue
				}

				var ival *string
				if val == nil {
					if null != nil {
						ival = null
					}
				} else {
					ival = &[]string{df.Series[colIdx].ValueString(*row, dataframe.DontLock)}[0]
				}

				if !exists {
					// Use series name
					rowData[seriesName] = ival
				} else {
					// Use provided column name
					rowData[*colName] = ival
				}
			}
		}

		batchData = append(batchData, rowData)

		if batchSize != nil && batchCount == *batchSize {
			// Now insert data to table
			err := sqlInsert(ctx, db, tableName, batchData)
			if err != nil {
				return err
			}

			batchCount = 0
			batchData = nil
		}

	}

	// Insert the remaining data into table
	if len(batchData) > 0 {
		err := sqlInsert(ctx, db, tableName, batchData)
		if err != nil {
			return err
		}
	}

	return nil
}

func sqlInsert(ctx context.Context, db execContexter, tableName string, batchData []map[string]*string) error {
	var query string

	fmt.Println("batchData", spew.Sdump(batchData))
	fmt.Println("------------")

	// Prepare Table Fields for insert query
	tableFields := []string{}
	for colName := range batchData[0] {
		tableFields = append(tableFields, colName)
	}

	fieldPlaceHolder := joinSliceToString(tableFields, false)

	query = query + "INSERT INTO " + tableName + "(" + fieldPlaceHolder + ") VALUES"

	// Prepare Values For Sql Insert
	for _, data := range batchData {
		values := []string{}
		// var valuesString string
		for _, field := range tableFields {
			values = append(values, *data[field])
		}

		valuesString := joinSliceToString(values, true)

		query = query + "(" + valuesString + "),"

	}

	// ready query statement
	query = strings.TrimSuffix(query, ",") + ";"
	fmt.Println(query)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

// joinSliceToString converts a slice of string
// To a comma separated list of string values
func joinSliceToString(fields []string, withQuotes bool) string {
	var fieldsStr string

	if withQuotes {
		for _, v := range fields {
			if v == "NULL" || v == "\\N" || v == "NaN" || v == "NA" {
				fieldsStr = fieldsStr + " " + "null" + ","
				continue
			}
			fieldsStr = fieldsStr + " '" + v + "',"
		}
	} else { // without quotes
		for _, v := range fields {
			if v == "NULL" || v == "\\N" || v == "NaN" || v == "NA" {
				fieldsStr = fieldsStr + " " + "null" + ","
				continue
			}
			fieldsStr = fieldsStr + " " + v + ","
		}
	}

	fieldsStr = strings.TrimSuffix(fieldsStr, ",")

	return fieldsStr
}
