package exports

import (
	"io"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// JSONExportOptions contains options for JSON
type JSONExportOptions struct {
	Comma   rune // Field delimiter (set to ',' by NewWriter)
	UseCRLF bool // True to use \r\n as the line terminator
}

// ExportToJSON exports data object to JSON
func ExportToJSON(w io.Writer, df dataframe.Dataframe, r ...dataframe.Range) error {

	return nil
}
