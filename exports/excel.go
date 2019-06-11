package exports

import (
	"context"
	"strings"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/tealeg/xlsx"
)

// EXCELExportOptions contains optional settings for EXCEL exporter functions
type EXCELExportOptions struct {
	NullString *string
	Range      dataframe.Range
}

// ExportToEXCEL exports df object to EXCEL
func ExportToEXCEL(ctx context.Context, filePath string, df *dataframe.DataFrame, options ...EXCELExportOptions) error {

	df.Lock()
	defer df.Unlock()

	// variables for excel sheet
	var sheetRow *xlsx.Row
	var file *xlsx.File
	// var sheet *xlsx.Sheet
	var cell *xlsx.Cell
	// var err error

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

		// Instantiale new excel file and select sheet
		file = xlsx.NewFile()
		sheet, err := file.AddSheet("Sheet1")
		if err != nil {
			return err
		}

		// Add first role to excel sheet
		// for header fields
		sheetRow = sheet.AddRow()
		// Write Header fields first
		for _, field := range df.Names() {
			cell = sheetRow.AddCell() // set column cell
			cell.Value = field        // assign field to cell
		}

		// Writing record in Rows
		for row := s; row <= e; row++ {

			// Add new role to excel sheet
			sheetRow = sheet.AddRow()
			// check if error in ctx context
			if err := ctx.Err(); err != nil {
				return err
			}

			// collecting rows
			// sVals := []string{}
			for _, aSeries := range df.Series {
				val := aSeries.Value(row)
				cell = sheetRow.AddCell()
				if val == nil {
					cell.Value = nullString
				} else {
					cell.Value = aSeries.ValueString(row)
				}
				// colCount := sheetRow.WriteSlice(&sVals, len(sVals))
				// if colCount == -1 {
				// 	fmt.Print()
				// 	return errors.New("A valid array slice pointer was not passed in to Excel Write Function")
				// }
			}

		}

		// For consistent file extension naming
		if strings.Contains(filePath, string('.')) {
			filePath = strings.Split(filePath, ".")[0]
		}
		if err = file.Save(filePath + ".xlsx"); err != nil {
			return err
		}
	}

	return nil
}
