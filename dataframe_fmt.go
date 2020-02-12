// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"bytes"
	"fmt"

	"github.com/olekukonko/tablewriter"
)

// TableOptions can be used to limit the number of rows and which Series
// are used when generating the table.
type TableOptions struct {

	// Series is used to display a given set of Series. When nil (default), all Series are displayed.
	// An index of the Series or the name of the Series can be provided.
	//
	// Example:
	//
	//  opts :=  TableOptions{Series: []interface{}{1, "time"}}
	//
	Series []interface{}

	// R is used to limit the range of rows.
	R *Range
}

// Table will produce the Dataframe in a table.
func (df *DataFrame) Table(opts ...TableOptions) string {

	df.lock.RLock()
	defer df.lock.RUnlock()

	if len(opts) == 0 {
		opts = append(opts, TableOptions{R: &Range{}})
	} else if opts[0].R == nil {
		opts[0].R = &Range{}
	}

	columns := map[interface{}]struct{}{}
	for _, v := range opts[0].Series {
		columns[v] = struct{}{}
	}

	data := [][]string{}

	headers := []string{""} // row header is blank
	footers := []string{fmt.Sprintf("%dx%d", df.n, len(df.Series))}
	for idx, aSeries := range df.Series {
		if len(columns) == 0 {
			headers = append(headers, aSeries.Name())
			footers = append(footers, aSeries.Type())
		} else {
			// Check idx
			_, exists := columns[idx]
			if exists {
				headers = append(headers, aSeries.Name())
				footers = append(footers, aSeries.Type())
				continue
			}

			// Check series name
			_, exists = columns[aSeries.Name()]
			if exists {
				headers = append(headers, aSeries.Name())
				footers = append(footers, aSeries.Type())
				continue
			}
		}
	}

	if df.n > 0 {
		s, e, err := opts[0].R.Limits(df.n)
		if err != nil {
			panic(err)
		}

		for row := s; row <= e; row++ {

			sVals := []string{fmt.Sprintf("%d:", row)}

			for idx, aSeries := range df.Series {
				if len(columns) == 0 {
					sVals = append(sVals, aSeries.ValueString(row))
				} else {
					// Check idx
					_, exists := columns[idx]
					if exists {
						sVals = append(sVals, aSeries.ValueString(row))
						continue
					}

					// Check series name
					_, exists = columns[aSeries.Name()]
					if exists {
						sVals = append(sVals, aSeries.ValueString(row))
						continue
					}
				}
			}

			data = append(data, sVals)
		}
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

// String will display Dataframe.
func (df *DataFrame) String() string {

	if df.NRows() <= 6 {
		return df.Table()
	}

	df.lock.RLock()
	defer df.lock.RUnlock()

	idx := []int{0, 1, 2, df.n - 3, df.n - 2, df.n - 1}

	data := [][]string{}

	headers := []string{""} // row header is blank
	footers := []string{fmt.Sprintf("%dx%d", df.n, len(df.Series))}
	for _, aSeries := range df.Series {
		headers = append(headers, aSeries.Name())
		footers = append(footers, aSeries.Type())
	}

	for j, row := range idx {

		if j == 3 {
			sVals := []string{"⋮"}

			for range df.Series {
				sVals = append(sVals, "⋮")
			}

			data = append(data, sVals)
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
