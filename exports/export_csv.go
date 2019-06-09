package exports

import (
	"encoding/csv"
	"fmt"
	"io"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// CSVExportOptions contains options for CSV
type CSVExportOptions struct {
	Comma   rune // Field delimiter (set to ',' by NewWriter)
	UseCRLF bool // True to use \r\n as the line terminator
}

// ExportToCSV exports data object to CSV
func ExportToCSV(w io.Writer, df dataframe.Dataframe, r ...dataframe.Range) error {
	header := []string{""}
	records := [][]string{}

	cw := csv.NewWriter(w)

	// if len(options) > 0 {
	// 	cw.Comma = options[0].Comma
	// 	cw.UseCRLF = options[0].UseCRLF
	// }

	if len(r) == 0 {
		r = append(r, dataframe.Range{})
	}

	for _, aSeries := range df.Series {
		header = append(header, aSeries.Name)
	}
	records = append(records, header)

	if df.n > 0 {
		s, e, err := r[0].Limits(df.n)
		if err != nil {
			return err
		}

		for row := s; row <= e; row++ {
			sVals := []string{fmt.Sprintf("%d:", row)}

			for _, aSeries := range df.Series {
				sVals = append(sVals, aSeries.ValueString(row))
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

}
