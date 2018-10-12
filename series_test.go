package dataframe

import (
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestSeriesRename(t *testing.T) {

	// Create new series
	init := []Series{

		NewSeriesFloat64("test", &SeriesInit{1, 0}),
		NewSeriesInt64("test", &SeriesInit{1, 0}),
		NewSeriesString("test", &SeriesInit{1, 0}),
		NewSeriesTime("test", &SeriesInit{1, 0}),
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
		NewSeriesFloat64("test", &SeriesInit{1, 0}),
		NewSeriesInt64("test", &SeriesInit{1, 0}),
		NewSeriesString("test", &SeriesInit{1, 0}),
		NewSeriesTime("test", &SeriesInit{1, 0}),
		NewSeries("test", civil.Date{}, &SeriesInit{1, 0}),
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
		NewSeriesFloat64("test", &SeriesInit{1, 0}, 1, nil, 2, 3),
		NewSeriesInt64("test", &SeriesInit{1, 0}, 1, nil, 2, 3),
		NewSeriesString("test", &SeriesInit{1, 0}, "1", nil, "2", "3"),
		NewSeriesTime("test", &SeriesInit{1, 0}, time.Now(), nil, time.Now(), time.Now()),
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

func TestSeriesOperations(t *testing.T) {

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", nil),
		NewSeriesInt64("test", nil),
		NewSeriesString("test", nil),
		NewSeriesTime("test", nil),
		NewSeries("test", civil.Date{}, nil),
	}

	tRef := time.Date(2017, 1, 1, 5, 30, 12, 0, time.UTC)

	// Append and Prepend value
	appendVals := []interface{}{
		1.0, 2.0, 3.0, 4.0,
		1, 2, 3, 4,
		"1", "2", "3", "4",
		tRef, tRef.Add(24 * time.Hour), tRef.Add(2 * 24 * time.Hour), tRef.Add(3 * 24 * time.Hour),
		civil.Date{2018, time.May, 1}, civil.Date{2018, time.May, 2}, civil.Date{2018, time.May, 3}, civil.Date{2018, time.May, 4},
	}

	for i := range init {
		s := init[i]
		s.Append(appendVals[4*i+0])
		s.Append(appendVals[4*i+1])
		s.Prepend(appendVals[4*i+2])
		s.Insert(s.NRows(), appendVals[4*i+3])
	}

	// Remove middle value
	for i := range init {
		s := init[i]
		s.Remove(1)
	}

	// Test Values
	expectedValues := [][]interface{}{
		[]interface{}{3.0, 2.0, 4.0},
		[]interface{}{3, 2, 4},
		[]interface{}{"3", "2", "4"},
		[]interface{}{tRef.Add(2 * 24 * time.Hour), tRef.Add(24 * time.Hour), tRef.Add(3 * 24 * time.Hour)},
		[]interface{}{civil.Date{2018, time.May, 3}, civil.Date{2018, time.May, 2}, civil.Date{2018, time.May, 4}},
	}

	for i := range init {
		s := init[i]

		exVals := expectedValues[i]
		for row := 0; row < len(exVals); row++ {
			rowVal := s.ValueString(row)
			exp := exVals[row]

			if rowVal != fmt.Sprintf("%v", exp) {
				t.Errorf("wrong type: expected: %v actual: %v", exp, rowVal)
			}
		}
	}
}
func TestSeriesCopy(t *testing.T) {

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{1, 0}, 1, nil, 2, 3),
		NewSeriesInt64("test", &SeriesInit{1, 0}, 1, nil, 2, 3),
		NewSeriesString("test", &SeriesInit{1, 0}, "1", nil, "2", "3"),
		NewSeriesTime("test", &SeriesInit{1, 0}, time.Now(), nil, time.Now(), time.Now()),
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
