package exports

import (
	"context"
	"io"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// EXCELExportOptions contains optional settings for EXCEL exporter functions
type EXCELExportOptions struct {
	NullString *string
	Range      dataframe.Range
}

// ExportToEXCEl exports df object to EXCEL
func ExportToEXCEl(ctx context.Context, w io.Writer, df *dataframe.DataFrame, options ...EXCELExportOptions) error {

	df.Lock()
	defer df.Unlock()

	nullString := "NaN" // Default value
	var r dataframe.Range

	if len(options) > 0 {
		r = options[0].Range
		if options[0].NullString != nil {
			nullString = *options[0].NullString
		}
	}

	if df.NRows(dataframe.NRowsOptions{DontLock: true}) > 0 {
		s, e, err := r.Limits(df.NRows(dataframe.NRowsOptions{DontLock: true}))
		if err != nil {
			return err
		}

		for row := s; row <= e; row++ {

			// check if error in ctx context
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}

	return nil
}
