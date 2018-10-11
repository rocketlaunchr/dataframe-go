package dataframe

import (
	"bytes"
	"fmt"

	"github.com/olekukonko/tablewriter"
)

// Table will produce the data in a table.
// s and e represent the range of rows to tabulate.
// They are both inclusive. A nil value means no limit.
func (df *DataFrame) Table(r ...Range) string {
	df.lock.RLock()
	defer df.lock.RUnlock()

	if len(r) == 0 {
		r = append(r, Range{})
	}

	var (
		s int
		e int
	)

	if r[0].Start == nil {
		s = 0
	} else {
		s = *r[0].Start
	}

	if r[0].End == nil {
		e = df.n - 1
	} else {
		e = *r[0].End
	}

	data := [][]string{}

	headers := []string{""} // row header is blank
	footers := []string{fmt.Sprintf("%dx%d", df.n, len(df.Series))}
	for _, aSeries := range df.Series {
		headers = append(headers, aSeries.Name())
		footers = append(footers, aSeries.Type())
	}

	for row := 0; row < df.n; row++ {

		if row > e {
			break
		}
		if row < s {
			continue
		}

		sVals := []string{fmt.Sprintf("%d:", row)}

		for _, aSeries := range df.Series {
			sVals = append(sVals, aSeries.ValueString(row))
		}

		data = append(data, sVals)
	}

	var buf bytes.Buffer

	table := tablewriter.NewWriter(&buf)
	table.SetHeader(headers)
	for _, v := range data {
		table.Append(v)
	}
	table.SetFooter(footers)
	table.SetAlignment(tablewriter.ALIGN_CENTER)

	table.Render()

	return buf.String()

}

// String will display dataframe
func (df *DataFrame) String() string {
	return df.Table()
}
