// Copyright 2019-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

// +build go1.15

package xseries

import "strconv"

func parseComplex(s string) (complex128, error) {
	return strconv.ParseComplex(s, 128)
}
