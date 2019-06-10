package exports

import (
	"encoding/json"
	"io"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// JSONExportOptions contains options for JSON
type JSONExportOptions struct {
	Range dataframe.Range
}

// ExportToJSON exports data object to JSON
func ExportToJSON(w io.Writer, df *dataframe.DataFrame, options ...JSONExportOptions) error {
	var r dataframe.Range

	enc := json.NewEncoder(w)

	records := []map[string]interface{}{}

	if len(options) > 0 {

		if options[0].Range != r {
			r = options[0].Range
		}

	}

	// TODO: prepare df.Series into Name and value
	// Struct interface
	// to be encoded by enc.

	for _, record := range records {
		if err := enc.Encode(record); err != nil {
			return err
		}
	}

	return nil
}
