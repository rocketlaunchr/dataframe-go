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

// Database is used to set the Database.
// Different databases have different syntax for placeholders etc.
type Database int

const (
	PostgreSQL Database = 0
	MySQL      Database = 1
)

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
	BatchSize *uint

	// SeriesToColumn is used to export a series (key of map) to a column in the table.
	// If the key does not exist, the series name is used by default.
	// If the column value is nil, the series is ignored for the purposes of exporting.
	SeriesToColumn map[string]*string

	// Database is used to set the Database.
	Database Database
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
// It is assumed to be a PostgreSQL database (for placeholder purposes), unless
// otherwise set to MySQL using the Options.
func ExportToSQL(ctx context.Context, db execContexter, df *dataframe.DataFrame, tableName string, options ...SQLExportOptions) error {

	df.Lock()
	defer df.Unlock()

	var (
		null      *string
		r         dataframe.Range
		pk        *PrimaryKey
		batchSize *uint
		database  Database
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
		database = options[0].Database
		if database != PostgreSQL && database != MySQL {
			return errors.New("invalid database")
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

	// Determine column names
	columnNames := []string{}

	if pk != nil {
		columnNames = append(columnNames, pk.PrimaryKey)
	}

	for _, seriesName := range df.Names() {

		colName, exists := seriesToColumn[seriesName]
		if exists && colName == nil {
			// Ignore column
			continue
		}

		if !exists {
			// Use series name
			columnNames = append(columnNames, seriesName)
		} else {
			// Use provided column name
			columnNames = append(columnNames, *colName)
		}
	}

	// Iterate over rows

	iterator := df.Values(dataframe.ValuesOptions{InitialRow: start, Step: 1, DontReadLock: true})

	var (
		batchData  []interface{}
		batchCount uint
	)

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

		// Insert primary key
		if pk != nil {
			if pk.Value != nil {
				batchData = append(batchData, pk.Value(*row, nRows))
			} else {
				batchData = append(batchData, nil)
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

				batchData = append(batchData, ival)
			}
		}

		if batchSize != nil && batchCount == *batchSize {
			// Now insert data to table
			err := sqlInsert(ctx, db, database, tableName, columnNames, batchData)
			if err != nil {
				return err
			}

			batchCount = 0
			batchData = nil
		}

	}

	// Insert the remaining data into table
	if len(batchData) > 0 {
		err := sqlInsert(ctx, db, database, tableName, columnNames, batchData)
		if err != nil {
			return err
		}
	}

	return nil
}

func sqlInsert(ctx context.Context, db execContexter, database Database, tableName string, columnNames []string, batchData []interface{}) error {
	// var query string

	fmt.Println("columnNames", spew.Sdump(columnNames))
	fmt.Println("batchData", spew.Sdump(batchData))
	fmt.Println("------------")

	// // Prepare Table Fields for insert query
	// tableFields := []string{}
	// for colName := range batchData[0] {
	// 	tableFields = append(tableFields, colName)
	// }

	// fieldPlaceHolder := joinSliceToString(tableFields, false)

	// query = query + "INSERT INTO " + tableName + "(" + fieldPlaceHolder + ") VALUES"

	// // Prepare Values For Sql Insert
	// for _, data := range batchData {
	// 	values := []string{}
	// 	// var valuesString string
	// 	for _, field := range tableFields {
	// 		values = append(values, *data[field])
	// 	}

	// 	valuesString := joinSliceToString(values, true)

	// 	query = query + "(" + valuesString + "),"

	// }

	// // ready query statement
	// query = strings.TrimSuffix(query, ",") + ";"
	// fmt.Println(query)

	// _, err := db.ExecContext(ctx, query)
	// if err != nil {
	// 	return err
	// }

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
