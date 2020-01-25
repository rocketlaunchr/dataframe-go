// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"fmt"
)

// RowError signifies that a particular row contained or generated an error.
type RowError struct {
	Row int
	Err error
}

// Error implements the error interface.
func (re *RowError) Error() string {
	return fmt.Sprintf("row: %d: %v", re.Row, re.Err)
}

// Unwrap implements the Wrapper interface.
func (re *RowError) Unwrap() error {
	return re.Err
}
