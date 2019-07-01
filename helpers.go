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

// DontLock is short-hand for various functions that permit disabling locking.
var DontLock = Options{DontLock: true}
