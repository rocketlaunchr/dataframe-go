// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package forecast

import (
	"fmt"
	"math"
)

// ConfidenceInterval represents an estimated range of values that includes
// the forecasted value within its bounds.
type ConfidenceInterval struct {
	// Upper bounds
	Upper float64

	// Lower bounds
	Lower float64

	// Normal can be set to signify that errors are normally distributed with a mean of zero.
	Normal bool
}

// NormalError returns the error, assuming it is normally distributed with a mean of zero.
func (c ConfidenceInterval) NormalError() float64 {
	return (c.Upper - c.Lower) / 2.0
}

// String implements fmt.Stringer interface.
func (c ConfidenceInterval) String() string {
	if c.Normal {
		return fmt.Sprintf("±%f", c.NormalError())
	}
	return fmt.Sprintf("[%+f, %+f]", c.Lower, c.Upper)
}

// MeanConfidenceInterval - see https://otexts.com/fpp2/prediction-intervals.html
func MeanConfidenceInterval(pred, level, sigmaHat float64, T uint) ConfidenceInterval {
	x := ConfidenceLevelToZ(level) * sigmaHat * math.Sqrt(float64(1+1/T))
	c := ConfidenceInterval{
		Lower:  pred - x,
		Upper:  pred + x,
		Normal: true,
	}
	return c
}

// NaïveConfidenceInterval - see https://otexts.com/fpp2/prediction-intervals.html
func NaïveConfidenceInterval(pred, level, sigmaHat float64, h uint) ConfidenceInterval {
	x := ConfidenceLevelToZ(level) * sigmaHat * math.Sqrt(float64(h))
	c := ConfidenceInterval{
		Lower:  pred - x,
		Upper:  pred + x,
		Normal: true,
	}
	return c
}

// SeasonalNaïveConfidenceInterval - see https://otexts.com/fpp2/prediction-intervals.html
func SeasonalNaïveConfidenceInterval(pred, level, sigmaHat float64, h, seasonalPeriod uint) ConfidenceInterval {
	k := float64(int64((h - 1) / seasonalPeriod))
	x := ConfidenceLevelToZ(level) * sigmaHat * math.Sqrt(k+1)
	c := ConfidenceInterval{
		Lower:  pred - x,
		Upper:  pred + x,
		Normal: true,
	}
	return c
}

// DriftConfidenceInterval - see https://otexts.com/fpp2/prediction-intervals.html
func DriftConfidenceInterval(pred, level, sigmaHat float64, T, h uint) ConfidenceInterval {
	x := ConfidenceLevelToZ(level) * sigmaHat * math.Sqrt(float64(h*(1+h/T)))
	c := ConfidenceInterval{
		Lower:  pred - x,
		Upper:  pred + x,
		Normal: true,
	}
	return c
}

// Confidence contains the confidence intervals for various confidence levels.
// The key must be between 0 and 1 (exclusive). A confidence level of 95%
// is represented by 0.95.
type Confidence map[float64]ConfidenceInterval

const (
	z50  = 0.6744897501960818
	z55  = 0.7554150263604694
	z60  = 0.8416212335729143
	z65  = 0.9345892910734802
	z66  = 0.9541652531461945
	z667 = 0.9680888458785382
	z67  = 0.9741138770593095
	z68  = 0.9944578832097534
	z70  = 1.0364333894937896
	z75  = 1.1503493803760083
	z80  = 1.2815515655446008
	z85  = 1.439531470938456
	z90  = 1.6448536269514724
	z95  = 1.9599639845400534
	z96  = 2.053748910631822
	z97  = 2.17009037758456
	z98  = 2.32634787404084
	z99  = 2.5758293035489
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
	case 0.50:
		return z50
	case 0.55:
		return z55
	case 0.60:
		return z60
	case 0.65:
		return z65
	case 0.66:
		return z66
	case 0.667:
		return z667
	case 0.67:
		return z67
	case 0.68:
		return z68
	case 0.70:
		return z70
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
	case 0.96:
		return z96
	case 0.97:
		return z97
	case 0.98:
		return z98
	case 0.99:
		return z99
	default:
		return math.Sqrt2 * math.Erfinv(level)
	}
}
