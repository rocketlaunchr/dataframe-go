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

var sqrt2 = math.Sqrt(2)

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
	return sqrt2 * math.Erfinv(level)
}
