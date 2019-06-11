package exports

import (
	"context"
	"encoding/json"
	"io"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// JSONExportOptions contains options for JSON
type JSONExportOptions struct {
	NullString    *string
	Range         dataframe.Range
	SetEscapeHTML bool
}

// ExportToJSON exports data object to JSON
func ExportToJSON(ctx context.Context, w io.Writer, df *dataframe.DataFrame, options ...JSONExportOptions) error {
	df.Lock()
	defer df.Unlock()

	var r dataframe.Range
	nullString := "NaN"

	enc := json.NewEncoder(w)

	if len(options) > 0 {

		r = options[0].Range
		enc.SetEscapeHTML(options[0].SetEscapeHTML)
		if options[0].NullString != nil {
			nullString = *options[0].NullString
		}
	}

	// only add the DontLock option when the dataframe, df is already in a locked state
	if df.NRows(dataframe.Options{DontLock: true}) > 0 {

		s, e, err := r.Limits(df.NRows(dataframe.Options{DontLock: true}))
		if err != nil {
			return err
		}

		for row := s; row <= e; row++ {

			// check if error in ctx context
			if err := ctx.Err(); err != nil {
				return err
			}

			record := map[string]interface{}{}
			for _, aSeries := range df.Series {
				if aSeries.Value(row) == nil {
					record[aSeries.Name()] = nullString
				} else {
					record[aSeries.Name()] = aSeries.Value(row)
				}
			}
			// fmt.Print(record)

			if err := enc.Encode(record); err != nil {
				return err
			}

		}
	}

	return nil
}
