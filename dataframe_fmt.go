package dataframe

import (
	"bytes"
	"fmt"

	"github.com/olekukonko/tablewriter"
)

// Table will produce the data in a table.
// s and e represent the range of rows to tabulate.
// They are both inclusive. A nil value means no limit.
func (df *DataFrame) Table(s interface{}, e interface{}) string {
	df.lock.RLock()
	defer df.lock.RUnlock()

	if s == nil {
		s = 0
	} else {
		s = s.(int)
	}

	if e == nil {
		e = df.n - 1
	} else {
		e = e.(int)
	}

	data := [][]string{}

	headers := []string{""} // row header is blank
	footers := []string{fmt.Sprintf("%dx%d", df.n, len(df.Series))}
	for _, aSeries := range df.Series {
		headers = append(headers, aSeries.Name())
		footers = append(footers, aSeries.Type())
	}

	for row := 0; row < df.n; row++ {

		if row > e.(int) {
			break
		}
		if row < s.(int) {
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
	return df.Table(nil, nil)
}
