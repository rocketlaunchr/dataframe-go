// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

// +build js appengine safe

package dataframe

import "math"

// nan returns NaN.
// See: https://golang.org/pkg/math/#NaN
func nan() float64 {
	return math.NaN()
}

// isNaN returns whether f is NaN.
// See: https://golang.org/pkg/math/#IsNaN
func isNaN(f float64) bool {
	return f != f
}

// isInf returns whether f is +Inf or -Inf.
func isInf(f float64, sign int) bool {
	return sign >= 0 && f > 1.797693134862315708145274237317043567981e+308 || sign <= 0 && f < -1.797693134862315708145274237317043567981e+308
}
