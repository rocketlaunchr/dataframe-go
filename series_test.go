package dataframe

import (
	"fmt"
	"strings"
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
		NewSeriesFloat64("test", &SeriesInit{1, 0}, 1.0, nil, 2.0, 3.0),
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
			t.Errorf("wrong val: expected: %v actual: %v", expected[i], s.NRows())
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
		{3.0, 2.0, 4.0},
		{3, 2, 4},
		{"3", "2", "4"},
		{tRef.Add(2 * 24 * time.Hour), tRef.Add(24 * time.Hour), tRef.Add(3 * 24 * time.Hour)},
		{civil.Date{2018, time.May, 3}, civil.Date{2018, time.May, 2}, civil.Date{2018, time.May, 4}},
	}

	for i := range init {
		s := init[i]

		exVals := expectedValues[i]
		for row := 0; row < len(exVals); row++ {
			rowVal := s.ValueString(row)
			exp := exVals[row]

			if rowVal != fmt.Sprintf("%v", exp) {
				t.Errorf("wrong val: expected: %v actual: %v", exp, rowVal)
			}
		}
	}
}

func TestSeriesUpdate(t *testing.T) {

	tRef := time.Date(2017, 1, 1, 5, 30, 12, 0, time.UTC)

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{1, 0}, 1.0, 2.0, 3.0),
		NewSeriesInt64("test", &SeriesInit{1, 0}, 1, 2, 3),
		NewSeriesString("test", &SeriesInit{1, 0}, "1", "2", "3"),
		NewSeriesTime("test", &SeriesInit{1, 0}, tRef, tRef.Add(24*time.Hour), tRef.Add(2*24*time.Hour)),
		NewSeries("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 1}, civil.Date{2018, time.May, 2}, civil.Date{2018, time.May, 3}),
	}

	// Update values
	for i := range init {
		s := init[i]

		switch s.Type() {
		case "float64":
			s.Update(0, 99.0)
		case "int64":
			s.Update(0, 99)
		case "string":
			s.Update(0, "99")
		case "time":
			s.Update(0, tRef.Add(99*24*time.Hour))
		case "civil.Date":
			s.Update(0, civil.Date{2018, time.May, 99})
		}

	}

	expectedValues := [][]interface{}{
		{99.0, 2.0, 3.0},
		{99, 2, 3},
		{"99", "2", "3"},
		{tRef.Add(99 * 24 * time.Hour), tRef.Add(24 * time.Hour), tRef.Add(2 * 24 * time.Hour)},
		{civil.Date{2018, time.May, 99}, civil.Date{2018, time.May, 2}, civil.Date{2018, time.May, 3}},
	}

	for i := range init {
		s := init[i]

		exVals := expectedValues[i]
		for row := 0; row < len(exVals); row++ {
			rowVal := s.ValueString(row)
			exp := exVals[row]

			if rowVal != fmt.Sprintf("%v", exp) {
				t.Errorf("wrong val: expected: %v actual: %v", exp, rowVal)
			}
		}
	}

}

func TestSeriesSwap(t *testing.T) {

	tRef := time.Date(2017, 1, 1, 5, 30, 12, 0, time.UTC)

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{1, 0}, 1.0, 2.0, 3.0),
		NewSeriesInt64("test", &SeriesInit{1, 0}, 1, 2, 3),
		NewSeriesString("test", &SeriesInit{1, 0}, "1", "2", "3"),
		NewSeriesTime("test", &SeriesInit{1, 0}, tRef, tRef.Add(24*time.Hour), tRef.Add(2*24*time.Hour)),
		NewSeries("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}),
	}

	expectedValues := [][]interface{}{
		{3.0, 2.0, 1.0},
		{3, 2, 1},
		{"3", "2", "1"},
		{tRef.Add(2 * 24 * time.Hour), tRef.Add(24 * time.Hour), tRef},
		{civil.Date{2018, time.May, 3}, civil.Date{2018, time.May, 2}, civil.Date{2018, time.May, 1}},
	}

	for i := range init {
		s := init[i]

		s.Lock()
		s.Swap(0, 2, Options{DontLock: true})
		s.Unlock()

		exVals := expectedValues[i]
		for row := 0; row < len(exVals); row++ {
			rowVal := s.ValueString(row)
			exp := exVals[row]

			if rowVal != fmt.Sprintf("%v", exp) {
				t.Errorf("wrong val: expected: %v actual: %v", exp, rowVal)
			}
		}
	}

}

func TestSeriesSort(t *testing.T) {

	tRef := time.Date(2017, 1, 1, 5, 30, 12, 0, time.UTC)

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{1, 0}, nil, 1.0, 2.0, 3.0, nil),
		NewSeriesInt64("test", &SeriesInit{1, 0}, nil, 1, 2, 3, nil),
		NewSeriesString("test", &SeriesInit{1, 0}, nil, "1", "2", "3", nil),
		NewSeriesTime("test", &SeriesInit{1, 0}, nil, tRef, tRef.Add(24*time.Hour), tRef.Add(2*24*time.Hour), nil),
		NewSeries("test", civil.Date{}, &SeriesInit{0, 1}, nil, civil.Date{2018, time.May, 01}, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}, nil),
	}

	// Set IsLessThanFunc(a, b interface{}) bool
	(init[4].(*SeriesGeneric)).SetIsLessThanFunc(nil)
	(init[4].(*SeriesGeneric)).SetIsLessThanFunc(func(a, b interface{}) bool {
		g1 := a.(civil.Date)
		g2 := b.(civil.Date)

		return g1.Before(g2)
	})

	// Sort values
	for i := range init {
		s := init[i]
		s.Sort(Options{SortDesc: true})
	}

	expectedValues := [][]interface{}{
		{3.0, 2.0, 1.0, "NaN", "NaN"},
		{3, 2, 1, "NaN", "NaN"},
		{"3", "2", "1", "NaN", "NaN"},
		{tRef.Add(2 * 24 * time.Hour), tRef.Add(24 * time.Hour), tRef, "NaN", "NaN"},
		{civil.Date{2018, time.May, 3}, civil.Date{2018, time.May, 2}, civil.Date{2018, time.May, 1}, "NaN", "NaN"},
	}

	for i := range init {
		s := init[i]

		exVals := expectedValues[i]
		for row := 0; row < len(exVals); row++ {
			rowVal := s.ValueString(row)
			exp := exVals[row]

			if rowVal != fmt.Sprintf("%v", exp) {
				t.Errorf("wrong val: expected: %v actual: %v", exp, rowVal)
			}
		}
	}

}

type Tabler interface {
	Table(r ...Range) string
}

func TestSeriesTable(t *testing.T) {

	tRef := time.Date(2017, 1, 1, 5, 30, 12, 0, time.UTC)

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{1, 0}, 1, 2.0, 3.0),
		NewSeriesInt64("test", &SeriesInit{1, 0}, 1, 2, 3),
		NewSeriesString("test", &SeriesInit{1, 0}, "1", "2", "3"),
		NewSeriesTime("test", &SeriesInit{1, 0}, tRef, tRef.Add(24*time.Hour), tRef.Add(2*24*time.Hour)),
		NewSeries("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}),
	}

	expected := []string{
		`+-----+---------+
|     |  TEST   |
+-----+---------+
| 0:  |    1    |
| 1:  |    2    |
| 2:  |    3    |
+-----+---------+
| 3X1 | FLOAT64 |
+-----+---------+`,
		`+-----+-------+
|     | TEST  |
+-----+-------+
| 0:  |   1   |
| 1:  |   2   |
| 2:  |   3   |
+-----+-------+
| 3X1 | INT64 |
+-----+-------+`,
		`+-----+--------+
|     |  TEST  |
+-----+--------+
| 0:  |   1    |
| 1:  |   2    |
| 2:  |   3    |
+-----+--------+
| 3X1 | STRING |
+-----+--------+`,
		`+-----+-------------------------------+
|     |             TEST              |
+-----+-------------------------------+
| 0:  | 2017-01-01 05:30:12 +0000 UTC |
| 1:  | 2017-01-02 05:30:12 +0000 UTC |
| 2:  | 2017-01-03 05:30:12 +0000 UTC |
+-----+-------------------------------+
| 3X1 |             TIME              |
+-----+-------------------------------+`,
		`+-----+------------+
|     |    TEST    |
+-----+------------+
| 0:  | 2018-05-01 |
| 1:  | 2018-05-02 |
| 2:  | 2018-05-03 |
+-----+------------+
| 3X1 | CIVIL DATE |
+-----+------------+`,
	}

	for i := range init {
		s := init[i]

		if v, ok := s.(Tabler); ok {

			if strings.TrimSpace(v.Table()) != strings.TrimSpace(expected[i]) {
				t.Errorf("wrong val: expected: %v actual: %v", expected[i], v.Table())
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
			t.Errorf("wrong val: expected: %v actual: %v", s, cp)
		}
	}

}
