// Copyright 2019-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package xseries

import (
	"testing"
)

type tcase struct {
	str       string
	expAnswer complex128
	expErr    error
}

func TestParseComplex(t *testing.T) {

	tests := []tcase{
		{
			str:       "99",
			expAnswer: complex(99, 0),
		},
		{
			str:       "99",
			expAnswer: complex(99, 0),
		},
		{
			str:       "-99",
			expAnswer: complex(-99, 0),
		},
		{
			str:       "1i",
			expAnswer: complex(0, 1),
		},
		{
			str:       "-1i",
			expAnswer: complex(0, -1),
		},
		{
			str:       "3-1i",
			expAnswer: complex(3, -1),
		},
		{
			str:       "3+1i",
			expAnswer: complex(3, 1),
		},
		{
			str:       "3-1i",
			expAnswer: complex(3, -1),
		},
		{
			str:       "3+1i",
			expAnswer: complex(3, 1),
		},
		{
			str:       "1i",
			expAnswer: complex(0, 1),
		},
		{
			str:       "-1i",
			expAnswer: complex(0, -1),
		},
		{
			str:       "3e3-1i",
			expAnswer: complex(3e3, -1),
		},
		{
			str:       "-3e3-1i",
			expAnswer: complex(-3e3, -1),
		},
		{
			str:       "3e3-1i",
			expAnswer: complex(3e3, -1),
		},
		{
			str:       "3e+3-1i",
			expAnswer: complex(3e+3, -1),
		},
		{
			str:       "-3e+3-1i",
			expAnswer: complex(-3e+3, -1),
		},
		{
			str:       "-3e+3-1i",
			expAnswer: complex(-3e+3, -1),
		},
		{
			str:       "3e+3-3e+3i",
			expAnswer: complex(3e+3, -3e+3),
		},
		{
			str:       "3e+3+3e+3i",
			expAnswer: complex(3e+3, 3e+3),
		},
	}

	for i, tc := range tests {

		got, gotErr := parseComplex(tc.str)
		if gotErr != nil {
			if tc.expErr == nil {
				t.Errorf("%d: |got: %v |expected: %v", i, gotErr, tc.expErr)
			}
		} else {
			if tc.expErr != nil {
				t.Errorf("%d: |got: %v |expected: %v", i, got, tc.expErr)
			} else {
				if got != tc.expAnswer {
					t.Errorf("%d: |got: %v |expected: %v", i, got, tc.expAnswer)
				}
			}
		}
	}

}
