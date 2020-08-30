// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

// Package exports provides functionality to save the data contained in a DataFrame into another format.
// It provides inverse functionality to the imports package.
package exports

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dbq/v2"
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

type execContexter interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// SQLExportOptions contains options for ExportToSQL function.
type SQLExportOptions struct {

	// NullString is used to set what nil values should be encoded to.
	// Common options are NULL, \N, NaN, NA.
	NullString *string

	// Range is used to export a subset of rows from the Dataframe.
	Range dataframe.Range

	// PrimaryKey is used if you want to generate custom values for the primary key
	PrimaryKey *PrimaryKey

	// BatchSize is used to insert data in batches.
	// It is recommended a transaction is used so if 1 batch-insert fails, then all
	// successfully inserted data can be rolled back.
	// If set, it must not be 0.
	BatchSize *uint

	// SeriesToColumn is used to map the series name to the table's column name.
	// The key of the map is the series name. Column names are case-sensitive.
	// If the key does not exist, the series name is used by default.
	// If the map value is nil, the series is ignored for the purposes of exporting.
	SeriesToColumn map[string]*string

	// Database is used to set the Database.
	Database Database
}

// PrimaryKey is used to generate custom values for the primary key
type PrimaryKey struct {

	// PrimaryKey is the column name of primary key
	PrimaryKey string

	// Value is a function that generates a primary key value given the row number
	// and number of rows in the Dataframe.
	// For auto-incrementing primary keys, nil can be returned.
	Value func(row int, n int) *string
}

// ExportToSQL exports a Dataframe to a SQL Database.
// It is assumed to be a PostgreSQL database (for placeholder purposes), unless
// otherwise set to MySQL using the Options.
//
// Example (gist):
//
//  import (
//  	stdSql "database/sql"
//
//  	"github.com/rocketlaunchr/dataframe-go/exports"
//  	sql "github.com/rocketlaunchr/mysql-go"
//  	"github.com/myesui/uuid"
//  )
//
//  func main() {
//
//  	p, _ := stdSql.Open("mysql", "user:password@/dbname")
//  	pool := &sql.DB{DB:p}
//
//  	conn, err := pool.Conn(ctx)
//  	defer conn.Close()
//
//  	tx, _ := conn.BeginTx(ctx, nil)
//
//  	opts := exports.SQLExportOptions{
//  		SeriesToColumn: map[string]*string{
//  			"Country": &[]string{"country"}[0],
//  			"Age":     &[]string{"age"}[0],
//  			"Id":      nil,
//  			"Date":    nil,
//  			"Amount":  nil,
//  		},
//  		PrimaryKey: &exports.PrimaryKey{
//  			PrimaryKey: "uuid",
//  			Value: func(row int, n int) *string {
//  				str := uuid.NewV4().String()
//  				return &str
//  			},
//  		},
//  		BatchSize: &[]uint{50}[0],
//  		Database:  exports.MySQL,
//  	}
//
//  	err = exports.ExportToSQL(ctx, tx, df, "test", opts)
//  	if err != nil {
//  		tx.Rollback()
//  		return
//  	}
//
//  	tx.Commit()
//  }
//
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

	for _, seriesName := range df.Names(dataframe.DontLock) {

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

	var (
		batchData  []interface{}
		batchCount uint
	)

	// Iterate over rows
	for row := start; row <= end; row++ {

		// context has been canceled
		if err := ctx.Err(); err != nil {
			return err
		}

		batchCount = batchCount + 1

		// Insert primary key
		if pk != nil {
			if pk.Value != nil {
				batchData = append(batchData, pk.Value(row, nRows))
			} else {
				batchData = append(batchData, nil)
			}
		}

		for _, series := range df.Series {
			val := series.Value(row, dataframe.DontLock)

			colName, exists := seriesToColumn[series.Name()]
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
				switch v := val.(type) {
				case time.Time:
					ival = &[]string{v.Format("2006-01-02 15:04:05")}[0]
				default:
					ival = &[]string{series.ValueString(row, dataframe.DontLock)}[0]
				}
			}

			batchData = append(batchData, ival)
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
	tableName = strings.Join(escapeNames(database, []string{tableName}), ",")
	columns := strings.Join(escapeNames(database, columnNames), ",")

	var placeholders string
	if database == MySQL {
		placeholders = dbq.Ph(len(columnNames), len(batchData)/len(columnNames), 0)
	} else {
		placeholders = dbq.Ph(len(columnNames), len(batchData)/len(columnNames), 0, dbq.PostgreSQL)
	}

	stmt := "INSERT INTO " + tableName + " (" + columns + ") VALUES " + placeholders

	_, err := dbq.E(ctx, db, stmt, nil, batchData...)
	if err != nil {
		return err
	}

	return nil
}

func escapeNames(database Database, names []string) []string {
	out := []string{}

	switch database {
	case MySQL:
		for _, v := range names {
			out = append(out, fmt.Sprintf("`%s`", v))
		}
	case PostgreSQL:
		for _, v := range names {
			out = append(out, fmt.Sprintf("\"%s\"", v))
		}
	default:
		out = names
	}

	return out
}
