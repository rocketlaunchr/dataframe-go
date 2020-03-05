// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

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

// Float64Range will return a sequence of float64 values starting at start.
func Float64Range(start, end, step float64) []float64 {
	out := []float64{start}

	if step == 0 {
		return out
	}

	for {
		newVal := out[len(out)-1] + step

		if step > 0 {
			if newVal > end {
				break
			}
		} else {
			if newVal < end {
				break
			}
		}

		out = append(out, newVal)
	}

	return out
}

// DontLock is short-hand for various functions that permit disabling locking.
var DontLock = dontLock
var dontLock = Options{DontLock: true}
