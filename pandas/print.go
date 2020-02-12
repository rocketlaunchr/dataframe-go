// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package pandas

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/olekukonko/tablewriter"
)

func printMap(headers []string, mp map[string][]interface{}) string {
	headers = append([]string{""}, headers...)

	keys := []string{}
	for k := range mp {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	table := tablewriter.NewWriter(&buf)
	table.SetHeader(headers)
	for _, k := range keys {
		tr := []string{k}
		for _, v := range mp[k] {
			tr = append(tr, fmt.Sprintf("%v", v))
		}
		table.Append(tr)
	}
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.Render()
	return buf.String()
}
