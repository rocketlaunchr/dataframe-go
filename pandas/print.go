// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package pandas

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/olekukonko/tablewriter"
)

func printMap(mp map[string]interface{}) string {
	keys := []string{}
	for k := range mp {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	table := tablewriter.NewWriter(&buf)
	for _, k := range keys {
		table.Append([]string{k, fmt.Sprintf("%v", mp[k])})
	}
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.Render()
	return buf.String()
}
