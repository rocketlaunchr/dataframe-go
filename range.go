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

// NRows returns the number of rows contained by Range.
// If End is nil, then length must be provided.
func (r *Range) NRows(length ...int) (int, error) {

	if len(length) > 0 {
		s, e, err := r.Limits(length[0])
		if err != nil {
			return 0, err
		}

		return e - s + 1, nil
	}

	if r.End == nil {
		return 0, errors.New("End is nil so length must be provided")
	}

	var s int

	if r.Start != nil {
		s = *r.Start
	}

	if s < 0 || *r.End < 0 {
		return 0, errors.New("range invalid")
	}

	if *r.End < s {
		return 0, errors.New("range invalid")
	}

	return *r.End - s + 1, nil
}

// Limits is used to return the start and end limits of a Range
// object for a given Dataframe or Series with length number of rows.
func (r *Range) Limits(length int) (s int, e int, _ error) {

	if length <= 0 {
		return 0, 0, errors.New("limit undefined")
	}

	if r.Start == nil {
		s = 0
	} else {
		if *r.Start < 0 {
			// negative
			s = length + *r.Start
		} else {
			s = *r.Start
		}
	}

	if r.End == nil {
		e = length - 1
	} else {
		if *r.End < 0 {
			// negative
			e = length + *r.End
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

	if s >= length || e >= length {
		return 0, 0, errors.New("range invalid")
	}

	return
}

// RangeFinite returns a Range that has a finite span.
func RangeFinite(start, end int) Range {
	return Range{
		Start: &start,
		End:   &end,
	}
}

// IntsToRanges will convert an already (ascending) ordered list of ints to a slice of Ranges.
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
