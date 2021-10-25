// Copyright 2019-21 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package xseries

import "strconv"

func parseComplex(s string) (complex128, error) {
	return strconv.ParseComplex(s, 128)
}
