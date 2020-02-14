// Copyright 2018-19 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.
package forecast

import (
	"errors"
)

// ErrNoRows signifies that the Series, Dataframe or import data
// contains no rows of data.
var ErrNoRows = errors.New("contains no rows")

// ErrMismatchLen signifies that there is a difference between the lengths
// of 2 series or 2 dataframes. Usually it means that they are expected
// to match.
var ErrMismatchLen = errors.New("mismatch length")

// ErrIndeterminate indicates that the result of a calculation is indeterminate.
var ErrIndeterminate = errors.New("calculation result: indeterminate")
