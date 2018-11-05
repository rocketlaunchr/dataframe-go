// Copyright 2018 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"bytes"
	"fmt"

	"github.com/olekukonko/tablewriter"
)

// Table will produce the dataframe in a table.
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

func (df *DataFrame) Head(n ...int){
	end := 5
	if len(n) > 0 {
		end = n[0]
	}
	fmt.Println(df.Table(Range{
		Start: nil,
		End: &end,
	}))
}

func (df *DataFrame) Tail(n ...int){
	start := 5
	if len(n) > 0 {
		start = n[0]
	}
	st := df.NRows() - start
	fmt.Println(df.Table(Range{
		Start: &st,
		End:   nil,
	}))
}
