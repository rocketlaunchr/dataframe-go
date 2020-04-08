// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package evaluation

import (
	"math"
)

func isInvalidFloat64(val float64) bool {
	if math.IsInf(val, 0) || math.IsNaN(val) {
		return true
	}
	return false
}
