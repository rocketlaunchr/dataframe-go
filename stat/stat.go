package stat

import (
	"github.com/rocketlaunchr/dataframe-go"
	"math"
)

type Stat interface {
	dataframe.Series
	Abs()
}

type StFloat64 struct {
	*dataframe.SeriesFloat64
}

type SeriesInt64 struct {
	*dataframe.SeriesInt64
}

func (s *StFloat64) Abs() {
	for key, value := range s.Values {
		val := math.Abs(*value)
		s.Values[key] = &val
	}
}

func (s *SeriesInt64) Abs() {
	for key, value := range s.Values {
		val := *value
		if val < 0 {
			val = val * -1
		}
		s.Values[key] = &val
	}
}
