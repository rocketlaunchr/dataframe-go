package exports

import (
	"encoding/csv"
	"io"

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
func ExportToCSV(w io.Writer, df *dataframe.DataFrame, options ...CSVExportOptions) error {
	header := []string{}
	records := [][]string{}

	r := dataframe.Range{} // initial default range r
	NullString := "NaN"    // Set default value for null strings

	cw := csv.NewWriter(w)

	if len(options) > 0 {
		cw.Comma = options[0].Separator
		cw.UseCRLF = options[0].UseCRLF

		if options[0].Range != r {
			r = options[0].Range
		}

		if options[0].NullString != NullString {
			NullString = options[0].NullString
		}
	}

	for _, aSeries := range df.Series {
		header = append(header, aSeries.Name())
	}
	records = append(records, header)

	if df.NRows() > 0 {
		s, e, err := r.Limits(df.NRows())
		if err != nil {
			return err
		}

		for row := s; row <= e; row++ {
			sVals := []string{}

			for _, aSeries := range df.Series {
				val := aSeries.ValueString(row)
				// if strings.ToLower(val) == "nan" || strings.ToLower(val) == "na" || strings.ToLower(val) == "null" {
				// 	val = string(NullString)
				// }
				sVals = append(sVals, val)
			}

			records = append(records, sVals)
		}
	}

	// Writing csv to writer object
	for _, record := range records {
		if err := cw.Write(record); err != nil {
			return err
		}
	}

	cw.Flush()
	if err := cw.Error(); err != nil {
		return err
	}

	return nil
}
