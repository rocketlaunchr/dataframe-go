// Copyright 2018 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import "errors"

// Range is used to specify a range.
// Both Start and End are inclusive.
// A nil value means no limit, so a Start of nil means 0
// and an End of nil means no limit.
// The End value must always be equal to or larger than Start.
// Negative values are acceptable.
type Range struct {
	Start *int
	End   *int
}

func (r *Range) limits(len int) (s int, e int, _ error) {

	if len <= 0 {
		return 0, 0, errors.New("limit undefined")
	}

	if r.Start == nil {
		s = 0
	} else {
		if *r.Start < 0 {
			// negative
			s = len + *r.Start
		} else {
			s = *r.Start
		}
	}

	if r.End == nil {
		e = 0
	} else {
		if *r.End < 0 {
			// negative
			e = len + *r.End
		} else {
			e = *r.End
		}
	}

	if s < 0 || e < 0 {
		return 0, 0, errors.New("range invalid")
	}

	if s > e {
		return 0, 0, errors.New("range invalid")
	}

	if s >= len || e >= len {
		return 0, 0, errors.New("range invalid")
	}

	return
}

// RangeFinite returns a Range that has a
// finite span.
func RangeFinite(start, end int) Range {
	return Range{
		Start: &start,
		End:   &end,
	}
}
