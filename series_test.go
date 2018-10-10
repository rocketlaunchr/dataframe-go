package dataframe

import (
	"testing"
	"time"
)

func TestSeriesRename(t *testing.T) {

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", nil),
		NewSeriesInt64("test", nil),
		NewSeriesString("test", nil),
		NewSeriesTime("test", nil),
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
		NewSeriesFloat64("test", nil),
		NewSeriesInt64("test", nil),
		NewSeriesString("test", nil),
		NewSeriesTime("test", nil),
	}

	expected := []string{
		"float64",
		"int64",
		"string",
		"time",
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
		NewSeriesFloat64("test", nil, 1, 2, 3),
		NewSeriesInt64("test", nil, 1, 2, 3),
		NewSeriesString("test", nil, "1", "2", "3"),
		NewSeriesTime("test", nil, time.Now(), time.Now(), time.Now()),
	}

	expected := []int{
		3,
		3,
		3,
		3,
	}

	for i := range init {
		s := init[i]

		if s.NRows() != expected[i] {
			t.Errorf("wrong type: expected: %v actual: %v", expected[i], s.NRows())
		}
	}

}
