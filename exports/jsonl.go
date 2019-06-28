package exports

import (
	"context"
	"encoding/json"
	"io"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// JSONExportOptions contains options for ExportToJSON function.
type JSONExportOptions struct {

	// NullString is used to set what nil values should be encoded to.
	// Common options are strings: NULL, \N, NaN, NA.
	// If not set, then null (non-string) is used.
	NullString *string

	// Range is used to export a subset of rows from the dataframe.
	Range dataframe.Range

	// SetEscapeHTML specifies whether problematic HTML characters should be escaped inside JSON quoted strings.
	// See: https://golang.org/pkg/encoding/json/#Encoder.SetEscapeHTML
	SetEscapeHTML bool
}

// ExportToJSON exports a dataframe in the jsonl format.
// Each line represents a row from the dataframe.
//
// See: http://jsonlines.org/ for more information.
func ExportToJSON(ctx context.Context, w io.Writer, df *dataframe.DataFrame, options ...JSONExportOptions) error {

	df.Lock()
	defer df.Unlock()

	var r dataframe.Range
	var null *string // default is null

	enc := json.NewEncoder(w)

	if len(options) > 0 {

		r = options[0].Range

		enc.SetEscapeHTML(options[0].SetEscapeHTML)

		if options[0].NullString != nil {
			null = options[0].NullString
		}
	}

	nRows := df.NRows(dataframe.DontLock)

	if nRows > 0 {

		s, e, err := r.Limits(nRows)
		if err != nil {
			return err
		}

		for row := s; row <= e; row++ {

			if err := ctx.Err(); err != nil {
				return err
			}

			record := map[string]interface{}{}
			for _, aSeries := range df.Series {

				fieldName := aSeries.Name()

				val := aSeries.Value(row)

				if val == nil && null != nil {
					record[fieldName] = null
				} else {
					record[fieldName] = val
				}
			}

			if err := enc.Encode(record); err != nil {
				return err
			}

		}
	}

	return nil
}
