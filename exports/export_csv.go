package exports

import (
	"context"
	"encoding/csv"
	"io"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// CSVExportOptions contains options for CSV
type CSVExportOptions struct {
	NullString *string         //optional param to specify what nil values should be encoded as (i.e. NULL, \N, NaN, NA etc)
	Range      dataframe.Range // Range of data subsets to write from dataframe
	Separator  rune            // Field delimiter (set to ',' by NewWriter)
	UseCRLF    bool            // True to use \r\n as the line terminator
}

// ExportToCSV exports data object to CSV
func ExportToCSV(ctx context.Context, w io.Writer, df *dataframe.DataFrame, options ...CSVExportOptions) error {
	// Lock Dataframe to 
	df.Lock()         // lock dataframe object
	defer df.Unlock() // unlock dataframe

	header := []string{}

	var r dataframe.Range // initial default range r

	nullString := "NaN" // Default will be "NaN"

	cw := csv.NewWriter(w)

	if len(options) > 0 {
		cw.Comma = options[0].Separator
		cw.UseCRLF = options[0].UseCRLF
		r = options[0].Range
		if options[0].NullString != nil {
			nullString = *options[0].NullString
		}
	}

	for _, aSeries := range df.Series {
		header = append(header, aSeries.Name())
	}
	if err := cw.Write(header); err != nil {
		return err
	}

	if df.NRows() > 0 {

		s, e, err := r.Limits(df.NRows())
		if err != nil {
			return err
		}

		refreshCount := 0 // Set up refresh counter
		for row := s; row <= e; row++ {

			// check if error in ctx context
			if err := ctx.Err(); err != nil {
				return err
			}

			refreshCount++
			// flush after every 100 writes
			if refreshCount > 100 { // flush in the 101th count
				cw.Flush()
				if err := cw.Error(); err != nil {
					return err
				}
				refreshCount = 1 // reset refreshCount
			}

			sVals := []string{}
			for _, aSeries := range df.Series {
				val := aSeries.Value(row)
				if val == nil {
					sVals = append(sVals, nullString)
				} else {
					sVals = append(sVals, aSeries.ValueString(row, dataframe.Options{DontLock: true}))
				}
			}

			// Write every row
			if err := cw.Write(sVals); err != nil {
				return err
			}
		}

	}

	// flush before exit
	cw.Flush()
	if err := cw.Error(); err != nil {
		return err
	}

	return nil
}
