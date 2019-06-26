package exports

import (
	"context"
	"strings"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/tealeg/xlsx"
)

// ExcelExportOptions contains optional settings for EXCEL exporter functions
type ExcelExportOptions struct {
	// optional param to specify what nil values should be encoded
	// as (i.e. NULL, \N, NaN, NA etc)
	NullString *string
	// Range of data subsets to write from dataframe
	Range dataframe.Range
	// Used to Specify the Specific Sheet page
	// To Write to in the Excel workspace
	// e.g sheet1, sheet2...
	WriteSheet *string
}

// ExportToEXCEL exports df object to EXCEL
func ExportToEXCEL(ctx context.Context, filePath string, df *dataframe.DataFrame, options ...ExcelExportOptions) error {

	df.Lock()
	defer df.Unlock()

	// variables for excel sheet
	var sheetRow *xlsx.Row
	var file *xlsx.File
	// var sheet *xlsx.Sheet
	var cell *xlsx.Cell
	// var err error

	nullString := "NaN"    // Default value
	writeSheet := "sheet1" // Write to default sheet 1 if a different one is not set

	var r dataframe.Range

	if len(options) > 0 {
		r = options[0].Range
		if options[0].NullString != nil {
			nullString = *options[0].NullString
		}

		if options[0].WriteSheet != nil {
			writeSheet = *options[0].WriteSheet
		}
	}

	if df.NRows(dataframe.Options{DontLock: true}) > 0 {
		s, e, err := r.Limits(df.NRows(dataframe.Options{DontLock: true}))
		if err != nil {
			return err
		}

		// Instantiale new excel file and select sheet
		file = xlsx.NewFile()
		sheet, err := file.AddSheet(writeSheet)
		if err != nil {
			return err
		}

		// Add first row to excel sheet
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
