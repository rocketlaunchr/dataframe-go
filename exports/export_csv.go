package exports

import (
	"context"
	"encoding/csv"
	"io"
	"strings"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// CSVExportOptions contains options for CSV
type CSVExportOptions struct {
	NullString string          //optional param to specify what nil values should be encoded as (i.e. NULL, \N, NaN, NA etc)
	Range      dataframe.Range // Range of data subsets to write from dataframe
	Separator  rune            // Field delimiter (set to ',' by NewWriter)
	UseCRLF    bool            // True to use \r\n as the line terminator
}

// ExportToCSV exports data object to CSV
func ExportToCSV(ctx context.Context, w io.Writer, df *dataframe.DataFrame, options ...CSVExportOptions) error {
	header := []string{}

	var r dataframe.Range  // initial default range r
	var NullString *string // Set default value for null strings

	cw := csv.NewWriter(w)

	if len(options) > 0 {
		cw.Comma = options[0].Separator
		cw.UseCRLF = options[0].UseCRLF
		r = options[0].Range
		NullString = &options[0].NullString
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

		df.Lock()         // lock dataframe object
		refreshCount := 0 // Set up refresh counter
		for row := s; row <= e; row++ {

			// check if error in ctx context
			if err := ctx.Err(); err != nil {
				return err
			}

			refreshCount++
			// flush after every 100 writes
			if refreshCount == 100 {
				cw.Flush()
				if err := cw.Error(); err != nil {
					return err
				}
				refreshCount = 0 // reset refreshCount
			}

			sVals := []string{}
			for _, aSeries := range df.Series {
				var val string
				v := aSeries.Value(row)
				if v == nil || strings.ToLower(v.(string)) == "nan" || strings.ToLower(v.(string)) == "na" {
					val = *NullString
				} else {
					val = v.(string) // Type assertion of interface to fetch string
				}
				sVals = append(sVals, val)
			}

			// Write every row
			if err := cw.Write(sVals); err != nil {
				return err
			}
		}
		df.Unlock() // unlock dataframe
	}

	// flush before exit
	cw.Flush()
	if err := cw.Error(); err != nil {
		return err
	}

	return nil
}
