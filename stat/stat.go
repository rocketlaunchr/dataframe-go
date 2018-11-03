package stat

import (
	"github.com/rocketlaunchr/dataframe-go"
	"math"
)

type SeriesRational interface {
	dataframe.Series
	Abs()
}

type SeriesFloat64 struct {
	dataframe.SeriesFloat64
}

type SeriesInt64 struct {
	dataframe.SeriesInt64
}

func (s SeriesFloat64) Abs() {
	for key, value := range s.Values {
		if s.Values[key] == nil {continue}
		val := math.Abs(*value)
		s.Values[key] = &val
	}
}

func (s *SeriesInt64) Abs() {
	for key, value := range s.Values {
		if s.Values[key] == nil {continue}
		val := *value
		if val < 0 {
			val = val * -1
		}
		s.Values[key] = &val
	}
}
