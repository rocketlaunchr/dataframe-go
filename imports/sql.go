// Copyright 2019 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"context"
	"database/sql"
	"io"

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
}

// LoadFromSQL will load data from a sql database.
// WARNING: The API may change in the future.
func LoadFromSQL(ctx context.Context, stmt *sql.Stmt, options ...SQLLoadOptions) (*dataframe.DataFrame, error) {

	panic("TODO: LoadFromSQL not implemented")

}
