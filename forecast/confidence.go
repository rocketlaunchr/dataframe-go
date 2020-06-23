// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package forecast

import "math"

// ConfidenceInterval represents an estimated range of values that includes
// the forecasted value within its bounds.
type ConfidenceInterval struct {

	// Upper bounds
	Upper float64

	// Lower bounds
	Lower float64
}

// Confidence contains the confidence intervals for various confidence levels.
// The key must be between 0 and 1 (exclusive). A confidence level of 95%
// is represented by 0.95.
type Confidence map[float64]ConfidenceInterval

var (
	sqrt2 = math.Sqrt(2)
	z5    = 0.6744897501960818
	z68   = 0.9944578832097534
	z75   = 1.1503493803760083
	z80   = 1.2815515655446008
	z85   = 1.439531470938456
	z90   = 1.6448536269514724
	z95   = 1.9599639845400534
	z98   = 2.32634787404084
	z99   = 2.5758293035489
)

// ConfidenceLevelToZ returns the Z value for a given confidence level.
// level must be between 0 and 1 (exclusive).
//
// level: 0.75 (75%) => 1.15 (approx)
//
// level: 0.95 (95%) => 1.96 (approx)
//
// level: 0.99 (99%) => 2.58 (approx)
//
// See: https://otexts.com/fpp2/prediction-intervals.html
func ConfidenceLevelToZ(level float64) float64 {
	switch level {
	case 0.5:
		return z5
	case 0.68:
		return z68
	case 0.75:
		return z75
	case 0.80:
		return z80
	case 0.85:
		return z85
	case 0.90:
		return z90
	case 0.95:
		return z95
	case 0.98:
		return z98
	case 0.99:
		return z99
	default:
		return sqrt2 * math.Erfinv(level)
	}
}
