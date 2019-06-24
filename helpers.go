// Copyright 2018 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"errors"
	"fmt"
)

// ErrNoRows signifies that the Series, Dataframe or import data
// contains no rows of data.
var ErrNoRows = errors.New("contains no rows")

const (
	// FALSE is used convert a false (bool) to an int.
	FALSE = 0
	// TRUE is used convert a true (bool) to an int.
	TRUE = 1
)

// B converts a boolean to an int.
func B(b bool) int {
	if b {
		return 1
	}
	return 0
}

// BoolValueFormatter is used by SetValueToStringFormatter
// to display an int as a bool. If the encountered value
// is not a 0 or 1, it will panic.
func BoolValueFormatter(v interface{}) string {
	if v == nil {
		return "NaN"
	}

	str := fmt.Sprintf("%v", v)
	switch str {
	case "0":
		return "false"
	case "1":
		return "true"
	default:
		_ = v.(bool) // Intentionally panic
		return ""
	}
}

// IntsToRanges will convert an already (asc) ordered list of ints to a slice of Ranges.
//
// Example:
//
//  import "sort"
//  ints := []int{2,4,5,6,8,10,11,45,46}
//  sort.Ints(ints)
//
//  fmt.Println(IntsToRanges(ints))
//  // Output: R{2,2}, R{4,6}, R{8,8}, R{10,11}, R{45,46}
//
func IntsToRanges(ints []int) []Range {

	out := []Range{}

OUTER:
	for i := 0; i < len(ints); i++ {
		v1 := ints[i]

		j := i + 1
		for {
			if j >= len(ints) {
				// j doesn't exist
				v2 := ints[j-1]
				out = append(out, Range{Start: &v1, End: &v2})
				break OUTER
			} else {
				// j does exist
				v2 := ints[j]
				prevVal := ints[j-1]

				if v2 != prevVal+1 {
					out = append(out, Range{Start: &v1, End: &prevVal})
					i = j - 1
					break
				}
				j++
				continue
			}
		}
	}

	return out
}
