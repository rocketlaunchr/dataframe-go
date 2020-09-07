// Copyright 2019-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

// +build !go1.15

package xseries

import "strconv"

func convErr(err error, s string) error {
	if x, ok := err.(*strconv.NumError); ok {
		x.Func = "ParseComplex"
		x.Num = s
	}
	return err
}

func parseComplex(s string) (complex128, error) {

	orig := s

	if len(s) == 0 {
		err := &strconv.NumError{
			Func: "ParseComplex",
			Num:  orig,
			Err:  strconv.ErrSyntax,
		}
		return 0, err
	}

	lastChar := s[len(s)-1:]

	// Remove brackets
	if len(s) > 1 && s[0:1] == "(" && lastChar == ")" {
		s = s[1 : len(s)-1]
		lastChar = s[len(s)-1:]
	}

	// Is last character an i?
	if lastChar != "i" {
		// The last character is not an i so there is only a real component.
		real, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, convErr(err, orig)
		}
		return complex(real, 0), nil
	}

	// Remove last char which is an i
	s = s[0 : len(s)-1]

	// Count how many ± exist.
	pos := []int{}

	for idx, rune := range s {
		if rune == '+' || rune == '-' {
			pos = append(pos, idx)
		}
	}

	if len(pos) == 0 {
		// There is only an imaginary component

		if s == "" {
			s = s + "1"
		}

		imag, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, convErr(err, orig)
		}
		return complex(0, imag), nil

	} else if len(pos) > 4 {
		// Too many ± exists for a valid complex number
		err := &strconv.NumError{
			Func: "ParseComplex",
			Num:  orig,
			Err:  strconv.ErrSyntax,
		}
		return 0, err
	}

	/* From here onwards, it is either a complex number with both a real and imaginary component OR a pure imaginary number in exponential form. */

	// Loop through pos from middle of slice, outwards
	mid := (len(pos) - 1) >> 1
	for j := 0; j < len(pos); j++ {
		var idx int
		if j%2 == 0 {
			idx = mid - j/2
		} else {
			idx = mid + (j/2 + 1)
		}

		left := s[0:pos[idx]]
		right := s[pos[idx]:]

		if left == "" {
			left = left + "0"
		}

		// Check if left and right are valid float64
		real, err := strconv.ParseFloat(left, 64)
		if err != nil {
			continue
		}

		if right == "+" || right == "-" {
			right = right + "1"
		}

		imag, err := strconv.ParseFloat(right, 64)
		if err != nil {
			continue
		}

		return complex(real, imag), nil
	}

	// Pure imaginary number in exponential form
	imag, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, convErr(err, orig)
	}
	return complex(0, imag), nil
}
