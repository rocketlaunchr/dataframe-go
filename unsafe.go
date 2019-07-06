// +build !js,!appengine,!safe

package dataframe

import (
	"unsafe"
)

// nan returns nan.
// See: https://golang.org/pkg/math/#NaN
func nan() float64 {
	uvnan := uint64(0x7FF8000000000001)
	return *(*float64)(unsafe.Pointer(&uvnan))
}

// isNaN returns whether f is NaN.
// See: https://golang.org/pkg/math/#IsNaN
func isNaN(f float64) bool {
	return f != f
}
