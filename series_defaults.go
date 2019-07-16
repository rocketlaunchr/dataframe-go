// Copyright 2018 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"fmt"
	"time"

	"github.com/google/go-cmp/cmp"
)

// DefaultIsEqualFunc is the default comparitor to determine if
// two values in the series are the same.
func DefaultIsEqualFunc(a, b interface{}) bool {
	return cmp.Equal(a, b)
}

// DefaultValueFormatter will return a string representation
// of the data in a particular row.
func DefaultValueFormatter(v interface{}) string {
	if v == nil {
		return "NaN"
	}

	switch val := v.(type) {
	case time.Time:
		return val.Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", v)
	}

}
