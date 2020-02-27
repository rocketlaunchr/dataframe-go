// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"context"
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
		NewSeriesMixed("test", &SeriesInit{1, 0}),
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{0, 1}),
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
		NewSeriesMixed("test", &SeriesInit{1, 0}),
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{1, 0}),
	}

	expected := []string{
		"float64",
		"int64",
		"string",
		"time",
		"mixed",
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
		NewSeriesMixed("test", &SeriesInit{1, 0}, 1, nil, 2, 3),
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, nil, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}),
	}

	expected := []int{
		4,
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
		NewSeriesMixed("test", nil),
		NewSeriesGeneric("test", civil.Date{}, nil),
	}

	tRef := time.Date(2017, 1, 1, 5, 30, 12, 0, time.UTC)

	// Append and Prepend value
	appendVals := []interface{}{
		1.0, 2.0, 3.0, 4.0,
		1, 2, 3, 4,
		"1", "2", "3", "4",
		tRef, tRef.Add(24 * time.Hour), tRef.Add(2 * 24 * time.Hour), tRef.Add(3 * 24 * time.Hour),
		1, 2, 3, 4,
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

	// Test values
	expectedValues := [][]interface{}{
		{3.0, 2.0, 4.0},
		{3, 2, 4},
		{"3", "2", "4"},
		{tRef.Add(2 * 24 * time.Hour), tRef.Add(24 * time.Hour), tRef.Add(3 * 24 * time.Hour)},
		{3, 2, 4},
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
		NewSeriesMixed("test", &SeriesInit{1, 0}, 1, 2, 3),
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 1}, civil.Date{2018, time.May, 2}, civil.Date{2018, time.May, 3}),
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
		case "mixed":
			s.Update(0, 99)
		case "civil.Date":
			s.Update(0, civil.Date{2018, time.May, 99})
		}

	}

	expectedValues := [][]interface{}{
		{99.0, 2.0, 3.0},
		{99, 2, 3},
		{"99", "2", "3"},
		{tRef.Add(99 * 24 * time.Hour), tRef.Add(24 * time.Hour), tRef.Add(2 * 24 * time.Hour)},
		{99, 2, 3},
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
		NewSeriesMixed("test", &SeriesInit{1, 0}, 1, 2, 3),
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}),
	}

	expectedValues := [][]interface{}{
		{3.0, 2.0, 1.0},
		{3, 2, 1},
		{"3", "2", "1"},
		{tRef.Add(2 * 24 * time.Hour), tRef.Add(24 * time.Hour), tRef},
		{3, 2, 1},
		{civil.Date{2018, time.May, 3}, civil.Date{2018, time.May, 2}, civil.Date{2018, time.May, 1}},
	}

	for i := range init {
		s := init[i]

		s.Lock()
		s.Swap(0, 2, DontLock)
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
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{0, 1}, nil, civil.Date{2018, time.May, 01}, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}, nil),
		//		NewSeriesMixed("test", &SeriesInit{1, 0}, nil, 1, 2, 3, nil),
	}

	// Set IsLessThanFunc(a, b interface{}) bool
	(init[4].(*SeriesGeneric)).SetIsLessThanFunc(nil)
	(init[4].(*SeriesGeneric)).SetIsLessThanFunc(func(a, b interface{}) bool {
		g1 := a.(civil.Date)
		g2 := b.(civil.Date)

		return g1.Before(g2)
	})

	// (init[5].(*SeriesMixed)).SetIsLessThanFunc(func(a, b interface{}) bool {

	// 	return b.(int) > a.(int)
	// })

	// Sort values
	for i := range init {
		s := init[i]
		s.Sort(context.Background(), SortOptions{Desc: true})
	}

	expectedValues := [][]interface{}{
		{3.0, 2.0, 1.0, "NaN", "NaN"},
		{3, 2, 1, "NaN", "NaN"},
		{"3", "2", "1", "NaN", "NaN"},
		{tRef.Add(2 * 24 * time.Hour), tRef.Add(24 * time.Hour), tRef, "NaN", "NaN"},
		{civil.Date{2018, time.May, 3}, civil.Date{2018, time.May, 2}, civil.Date{2018, time.May, 1}, "NaN", "NaN"},
		// {3, 2, 1, "NaN", "NaN"},
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
	String() string
}

func TestSeriesTable(t *testing.T) {

	tRef := time.Date(2017, 1, 1, 5, 30, 12, 0, time.UTC)

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{1, 0}, 1.0, 2.0, 3.0),
		NewSeriesInt64("test", &SeriesInit{1, 0}, 1, 2, 3),
		NewSeriesString("test", &SeriesInit{1, 0}, "1", "2", "3"),
		NewSeriesTime("test", &SeriesInit{1, 0}, tRef, tRef.Add(24*time.Hour), tRef.Add(2*24*time.Hour)),
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}),
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

func TestSeriesString(t *testing.T) {

	tRef := time.Date(2017, 1, 1, 5, 30, 12, 0, time.UTC)

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{1, 0}, 1.0, 2.0, 3.0),
		NewSeriesInt64("test", &SeriesInit{1, 0}, 1, 2, 3),
		NewSeriesString("test", &SeriesInit{1, 0}, "1", "2", "3"),
		NewSeriesTime("test", &SeriesInit{1, 0}, tRef, tRef.Add(24*time.Hour), tRef.Add(2*24*time.Hour)),
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}),

		NewSeriesFloat64("test", &SeriesInit{1, 0}, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0),
		NewSeriesInt64("test", &SeriesInit{1, 0}, 1, 2, 3, 4, 5, 6, 7),
		NewSeriesString("test", &SeriesInit{1, 0}, "1", "2", "3", "4", "5", "6", "7"),
		NewSeriesTime("test", &SeriesInit{1, 0}, tRef, tRef.Add(24*time.Hour), tRef.Add(2*24*time.Hour), tRef.Add(3*24*time.Hour), tRef.Add(4*24*time.Hour), tRef.Add(5*24*time.Hour), tRef.Add(6*24*time.Hour)),
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}, civil.Date{2018, time.May, 04}, civil.Date{2018, time.May, 05}, civil.Date{2018, time.May, 06}, civil.Date{2018, time.May, 07}),
	}

	expected := []string{`[ 1 2 3 ]`,
		`[ 1 2 3 ]`,
		`[ 1 2 3 ]`,
		`[ 2017-01-01 05:30:12 +0000 UTC 2017-01-02 05:30:12 +0000 UTC 2017-01-03 05:30:12 +0000 UTC ]`,
		`[ 2018-05-01 2018-05-02 2018-05-03 ]`,
		`[ 1 2 3 ... 5 6 7 ]`,
		`[ 1 2 3 ... 5 6 7 ]`,
		`[ 1 2 3 ... 5 6 7 ]`,
		`[ 2017-01-01 05:30:12 +0000 UTC 2017-01-02 05:30:12 +0000 UTC 2017-01-03 05:30:12 +0000 UTC ... 2017-01-05 05:30:12 +0000 UTC 2017-01-06 05:30:12 +0000 UTC 2017-01-07 05:30:12 +0000 UTC ]`,
		`[ 2018-05-01 2018-05-02 2018-05-03 ... 2018-05-05 2018-05-06 2018-05-07 ]`,
	}

	for i := range init {
		s := init[i]

		if v, ok := s.(Tabler); ok {

			if strings.TrimSpace(v.String()) != strings.TrimSpace(expected[i]) {
				t.Errorf("wrong val: expected: %v actual: %v", expected[i], v.String())
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
		NewSeriesMixed("test", &SeriesInit{1, 0}, 1, nil, 2, 3),
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, nil, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}),
	}

	for i := range init {
		s := init[i]

		cp := s.Copy()

		if !cmp.Equal(s, cp, cmpopts.EquateNaNs(), cmpopts.IgnoreUnexported(SeriesFloat64{}, SeriesInt64{}, SeriesString{}, SeriesTime{}, SeriesMixed{}, SeriesGeneric{})) {
			t.Errorf("wrong val: expected: %v actual: %v", s, cp)
		}
	}

}

func TestToSeriesString(t *testing.T) {
	ctx := context.Background()

	sm := NewSeriesMixed("test", &SeriesInit{1, 0}, 1, nil, 2, 3)
	ss, err := sm.ToSeriesString(ctx, false)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	// convert SeriesString back to SeriesMixed
	_, err = ss.ToSeriesMixed(ctx, false)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

}

func TestSeriesIsEqual(t *testing.T) {
	ctx := context.Background()
	tRef := time.Date(2017, 1, 1, 5, 30, 12, 0, time.UTC)

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{1, 0}, 1.0, 2.0, 3.0),
		NewSeriesInt64("test", &SeriesInit{1, 0}, 1, 2, 3),
		NewSeriesString("test", &SeriesInit{1, 0}, "1", "2", "3"),
		NewSeriesTime("test", &SeriesInit{1, 0}, tRef, tRef.Add(24*time.Hour), tRef.Add(2*24*time.Hour)),
		// NewSeriesMixed("test", &SeriesInit{1, 0}, 1, "two", 3.0),
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}),
	}

	(init[4].(*SeriesGeneric)).SetIsEqualFunc(func(a, b interface{}) bool {
		g1 := a.(civil.Date)
		g2 := b.(civil.Date)

		eq := !g1.After(g2) && !g1.Before(g2)
		fmt.Println(eq)
		return eq
	})

	expected := []Series{
		NewSeriesFloat64("expected", &SeriesInit{1, 0}, 1.0, 2.0, 3.0),
		NewSeriesInt64("expected", &SeriesInit{1, 0}, 1, 2, 3),
		NewSeriesString("expected", &SeriesInit{1, 0}, "1", "2", "3"),
		NewSeriesTime("expected", &SeriesInit{1, 0}, tRef, tRef.Add(24*time.Hour), tRef.Add(2*24*time.Hour)),
		// NewSeriesMixed("expected", &SeriesInit{1, 0}, 1, "two", 3.0),
		NewSeriesGeneric("expected", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}),
	}

	for i := range init {
		s1 := init[i]
		s2 := expected[i]

		eq, err := s1.IsEqual(ctx, s2)
		if err != nil {
			t.Errorf("error encountered: %s\n", err)
		}

		if !eq {
			t.Errorf("s1: [%T] %v is not equal to s2: [%T] %v\n", s1, s1, s2, s2)
		}

	}

}

func TestStopAtOneNil(t *testing.T) {

	tRef := time.Date(2017, 1, 1, 5, 30, 12, 0, time.UTC)

	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{1, 0}, 1.0, 2.0, 3.0, nil, 5.9, nil),
		NewSeriesInt64("test", &SeriesInit{1, 0}, 1, nil, nil, 2, 3),
		NewSeriesString("test", &SeriesInit{1, 0}, nil, "1", "2", nil, "3"),
		NewSeriesTime("test", &SeriesInit{1, 0}, tRef, nil, nil, tRef.Add(24*time.Hour), nil, tRef.Add(2*24*time.Hour)),
		NewSeriesMixed("test", &SeriesInit{1, 0}, 1, "two", nil, nil, 3.0, nil, nil),
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, nil, nil, civil.Date{2018, time.May, 02}, nil, nil, civil.Date{2018, time.May, 03}),
	}

	opts := NilCountOptions{StopAtOneNil: true}

	for i := range init {
		cnt, err := init[i].NilCount(opts)
		if err != nil {
			t.Errorf("error encountered: %s\n", err)
		}

		if cnt < 1 {
			t.Errorf("error: stop-at-one-nil functionality not working for Series: %T \n", init[i])
		}
	}
}
