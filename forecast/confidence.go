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
}

// String implements fmt.Stringer interface.
func (c ConfidenceInterval) String() string {
	return fmt.Sprintf("[%+f, %+f]", c.Lower, c.Upper)
}

// MeanConfidenceInterval - see https://otexts.com/fpp2/prediction-intervals.html
func MeanConfidenceInterval(pred, level, sigma_hat, T float64) ConfidenceInterval {
	x := ConfidenceLevelToZ(level) * sigma_hat * math.Sqrt(1+1/T)
	c := ConfidenceInterval{
		Lower: pred - x,
		Upper: pred + x,
	}
	return c
}

// NaïveConfidenceInterval - see https://otexts.com/fpp2/prediction-intervals.html
func NaïveConfidenceInterval(pred, level, sigma_hat, h float64) ConfidenceInterval {
	x := ConfidenceLevelToZ(level) * sigma_hat * math.Sqrt(h)
	c := ConfidenceInterval{
		Lower: pred - x,
		Upper: pred + x,
	}
	return c
}

// SeasonalNaïveConfidenceInterval - see https://otexts.com/fpp2/prediction-intervals.html
func SeasonalNaïveConfidenceInterval(pred, level, sigma_hat, h, seasonalPeriod float64) ConfidenceInterval {
	k := float64(int64((h - 1) / seasonalPeriod))
	x := ConfidenceLevelToZ(level) * sigma_hat * math.Sqrt(k+1)
	c := ConfidenceInterval{
		Lower: pred - x,
		Upper: pred + x,
	}
	return c
}

// DriftConfidenceInterval - see https://otexts.com/fpp2/prediction-intervals.html
func DriftConfidenceInterval(pred, level, sigma_hat, T, h float64) ConfidenceInterval {
	x := ConfidenceLevelToZ(level) * sigma_hat * math.Sqrt(h*(1+h/T))
	c := ConfidenceInterval{
		Lower: pred - x,
		Upper: pred + x,
	}
	return c
}

// Confidence contains the confidence intervals for various confidence levels.
// The key must be between 0 and 1 (exclusive). A confidence level of 95%
// is represented by 0.95.
type Confidence map[float64]ConfidenceInterval

const (
	z50 = 0.6744897501960818
	z55 = 0.7554150263604694
	z60 = 0.8416212335729143
	z65 = 0.9345892910734802
	z68 = 0.9944578832097534
	z70 = 1.0364333894937896
	z75 = 1.1503493803760083
	z80 = 1.2815515655446008
	z85 = 1.439531470938456
	z90 = 1.6448536269514724
	z95 = 1.9599639845400534
	z96 = 2.053748910631822
	z97 = 2.17009037758456
	z98 = 2.32634787404084
	z99 = 2.5758293035489
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
