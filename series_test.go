package dataframe

import (
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestSeriesRename(t *testing.T) {

	// Create new series
	init := []Series{

		NewSeriesFloat64("test", &SeriesInit{0, 1}),
		NewSeriesInt64("test", &SeriesInit{0, 1}),
		NewSeriesString("test", &SeriesInit{0, 1}),
		NewSeriesTime("test", &SeriesInit{0, 1}),
		NewSeries("test", civil.Date{}, &SeriesInit{0, 1}),
	}

	for i := range init {
		s := init[i]

		// Rename series
		s.Rename("test2")

		if s.Name() != "test2" {
			t.Errorf("wrong name")
		}
	}
}

func TestSeriesType(t *testing.T) {

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{0, 1}),
		NewSeriesInt64("test", &SeriesInit{0, 1}),
		NewSeriesString("test", &SeriesInit{0, 1}),
		NewSeriesTime("test", &SeriesInit{0, 1}),
		NewSeries("test", civil.Date{}, &SeriesInit{0, 1}),
	}

	expected := []string{
		"float64",
		"int64",
		"string",
		"time",
		"civil.Date",
	}

	for i := range init {
		s := init[i]

		if s.Type() != expected[i] {
			t.Errorf("wrong type: expected: %v actual: %v", expected[i], s.Type())
		}
	}
}

func TestSeriesNRows(t *testing.T) {

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{0, 1}, 1, nil, 2, 3),
		NewSeriesInt64("test", &SeriesInit{0, 1}, 1, nil, 2, 3),
		NewSeriesString("test", &SeriesInit{0, 1}, "1", nil, "2", "3"),
		NewSeriesTime("test", &SeriesInit{0, 1}, time.Now(), nil, time.Now(), time.Now()),
		NewSeries("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, nil, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}),
	}

	expected := []int{
		4,
		4,
		4,
		4,
		4,
	}

	for i := range init {
		s := init[i]

		if s.NRows() != expected[i] {
			t.Errorf("wrong type: expected: %v actual: %v", expected[i], s.NRows())
		}
	}

}

func TestSeriesCopy(t *testing.T) {

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{0, 1}, 1, nil, 2, 3),
		NewSeriesInt64("test", &SeriesInit{0, 1}, 1, nil, 2, 3),
		NewSeriesString("test", &SeriesInit{0, 1}, "1", nil, "2", "3"),
		NewSeriesTime("test", &SeriesInit{0, 1}, time.Now(), nil, time.Now(), time.Now()),
		NewSeries("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, nil, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}),
	}

	for i := range init {
		s := init[i]

		cp := s.Copy()

		if !cmp.Equal(s, cp, cmpopts.IgnoreUnexported(SeriesFloat64{}, SeriesInt64{}, SeriesString{}, SeriesTime{}, SeriesGeneric{})) {
			t.Errorf("wrong type: expected: %v actual: %v", s, cp)
		}
	}

}
