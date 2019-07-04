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
