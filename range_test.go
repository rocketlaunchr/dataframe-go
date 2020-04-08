// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"testing"
)

type tcase struct {
	Start *int
	End   *int
	ExpN  int
	ExpS  int
	ExpE  int
}

func TestRange(t *testing.T) {

	vals := []int{0, 1, 2, 3}

	N := len(vals)

	i := func(i int) *int {
		return &i
	}

	tests := []tcase{
		{
			Start: nil,
			End:   nil,
			ExpN:  4,
			ExpS:  0,
			ExpE:  3,
		},
		{
			Start: nil,
			End:   i(-1),
			ExpN:  4,
			ExpS:  0,
			ExpE:  3,
		},
		{
			Start: nil,
			End:   i(-2),
			ExpN:  3,
			ExpS:  0,
			ExpE:  2,
		},
		{
			Start: i(-3),
			End:   i(-2),
			ExpN:  2,
			ExpS:  1,
			ExpE:  2,
		},
	}

	for i, tc := range tests {

		rng := &Range{Start: tc.Start, End: tc.End}

		nrows, err := rng.NRows(N)
		if err != nil {
			panic(err)
		}
		if nrows != tc.ExpN {
			t.Errorf("%d: |got: %v |expected: %v", i, nrows, tc.ExpN)
		}

		s, e, err := rng.Limits(N)
		if err != nil {
			panic(err)
		}
		if s != tc.ExpS || e != tc.ExpE {
			t.Errorf("%d: |got: %v,%v |expected: %v,%v", i, s, e, tc.ExpS, tc.ExpE)
		}
	}

}
