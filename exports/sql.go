package exports

import (
	"context"
	"database/sql"
	"strings"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// ExecContexter interface allows for Transctions
// and non-transactions
type ExecContexter interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// SQLExportOptions contains options for ExportToSQL function.
type SQLExportOptions struct {
	// Range is used to export a subset of rows from the dataframe.
	Range dataframe.Range
}

// PrimaryKey is optional attribute for Primary key generator
type PrimaryKey struct {
	PrimaryKey string                       // field name of primary key
	Value      func(row int, n int) *string // we provide current row and number of rows in series and user will provide a value back (or nil)
}

// ExportToSQL exports a dataframe to an SQL Database.
func ExportToSQL(ctx context.Context, db ExecContexter, tableName string, df *dataframe.DataFrame, options ...SQLExportOptions) error {
	df.Lock()
	defer df.Unlock()

	var r dataframe.Range

	if len(options) > 0 {
		r = options[0].Range
	}

	nRows := df.NRows(dataframe.DontLock)

	if nRows > 0 {

		start, end, err := r.Limits(nRows)
		if err != nil {
			return err
		}

		// Fetch column Headers from Dataframe
		colHeaders := []string{}
		for _, aSeries := range df.Series {
			colHeaders = append(colHeaders, aSeries.Name())
		}

		insertFields := joinSliceToString(colHeaders, false)

		records := []map[string]string{}
		for row := start; row <= end; row++ {

			dataRow := map[string]string{}

			if err := ctx.Err(); err != nil {
				return err
			}

			for idx, aSeries := range df.Series {
				val := aSeries.Value(row, dataframe.DontLock)
				if val == nil {
					dataRow[colHeaders[idx]] = "null"
				} else {
					dataRow[colHeaders[idx]] = aSeries.ValueString(row, dataframe.DontLock)
				}
			}

			records = append(records, dataRow)
			// Bulk insert DataRows at a maximum of 50 rows
			// or last iteration for `row`
			if len(records) >= 50 || row == end {

				for _, record := range records {
					queryArgs := []string{}

					for _, field := range colHeaders {
						queryArgs = append(queryArgs, record[field])
					}
					query := "INSERT INTO " + tableName + "(" + insertFields + ") VALUES(" + joinSliceToString(queryArgs, true) + ");"

					// Run insert Query for row
					_, err := db.ExecContext(ctx, query)
					if err != nil {
						return err
					}

				}

				// re-initialise records
				records = []map[string]string{}
			}

		}
	}

	return nil
}

// joinSliceToString converts a slice of string
// To a comma separated list of string values
func joinSliceToString(fields []string, withQuotes bool) string {
	var fieldsStr string
	if withQuotes {
		for _, v := range fields {
			if v == "null" {
				fieldsStr = fieldsStr + " " + v + ","
				continue
			}
			fieldsStr = fieldsStr + " '" + v + "',"
		}
	} else { // without quotes
		for _, v := range fields {
			fieldsStr = fieldsStr + " " + v + ","
		}
	}

	fieldsStr = strings.TrimSuffix(fieldsStr, ",")

	return fieldsStr
}
