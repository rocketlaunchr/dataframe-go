package dataframe

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
)

func TestSeriesFilter(t *testing.T) {
	ctx := context.Background()

	s := NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2, 23.4, 56.3, 37.56, 99.09)

	filterFn := FilterSeriesFn(func(val interface{}, row, nRows int) (FilterAction, error) {
		sale := val.(float64)
		if sale <= 50.3 {
			return DROP, nil
		}
		return KEEP, nil
	})

	// Test Inplace: false
	fs, err := Filter(ctx, s, filterFn, FilterOptions{InPlace: false})
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	// Test Inplace: true
	_, err = Filter(ctx, s, filterFn, FilterOptions{InPlace: true})
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	actual := []*SeriesFloat64{s, fs.(*SeriesFloat64)}

	expected := `+-----+---------+
|     |  SALES  |
+-----+---------+
| 0:  |  56.2   |
| 1:  |  56.3   |
| 2:  |  99.09  |
+-----+---------+
| 3X1 | FLOAT64 |
+-----+---------+`

	for _, sc := range actual {
		if strings.TrimSpace(sc.Table()) != strings.TrimSpace(expected) {
			t.Errorf("wrong val: expected:\n%v\n actual:\n%v\n", expected, sc.Table())
		}
	}

}

func TestSeriesMeanStat(t *testing.T) {

	ctx := context.Background()

	initFs := []Series{
		NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2, nil, 56.3, nil, 99.09, 14),
		NewSeriesFloat64("sales", &SeriesInit{3, 0}),
	}

	initIs := []Series{
		NewSeriesInt64("test", nil, 1, nil, 20, 13, 87, nil, 34, 23),
		NewSeriesInt64("sales", &SeriesInit{3, 0}),
	}

	actual := []float64{}

	for i := range initFs {

		mSf, err := initFs[i].(*SeriesFloat64).Mean(ctx)
		if err != nil {
			t.Errorf("error encountered: %s\n", err)
		}
		if i == 0 {
			actual = append(actual, mSf)
		}

	}

	for i := range initFs {

		mSi, err := initIs[i].(*SeriesInt64).Mean(ctx)
		if err != nil {
			t.Errorf("error encountered: %s\n", err)
		}

		if i == 0 {
			actual = append(actual, mSi)
		}
	}

	expected := []float64{
		49.88166666666666,
		29.666666666666668,
	}

	if !cmp.Equal(expected, actual) {
		t.Errorf("wrong val: expected: %T %v actual: %T %v\n", expected, expected, actual, actual)
	}

}

func TestSeriesNilCount(t *testing.T) {

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{7, 0}, []float64{1.56, 3.4, 22.5}),
		NewSeriesInt64("test", &SeriesInit{3, 0}, []int64{3, 8, 4}),
		NewSeriesString("test", &SeriesInit{6, 0}, []string{"one", "three", "two"}),
		NewSeriesMixed("test", &SeriesInit{7, 0}, []interface{}{"3", 2.7, nil, 6}),
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, nil, nil, civil.Date{2018, time.May, 02}, nil),
		NewSeriesTime("test", &SeriesInit{3, 0}),
	}

	actual1 := []int{}
	for i := range init {
		if init[i].ContainsNil() {
			numNil, err := init[i].NilCount()
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			actual1 = append(actual1, numNil)
		}
	}

	expected1 := []int{4, 3, 4, 3, 3}

	for j := range expected1 {
		if actual1[j] != expected1[j] {
			t.Errorf("error: [id->%d] actual [%d] is not equal to expected [%d]\n", j, actual1[j], expected1[j])
		}
	}

	opts := NilCountOptions{
		Ctx:      context.Background(),
		R:        &Range{Start: &[]int{1}[0]},
		DontLock: true,
	}

	actual2 := []int{}
	for i := range init {
		if init[i].ContainsNil() {
			numNil, err := init[i].NilCount(opts)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			actual2 = append(actual2, numNil)
		}
	}
	expected2 := []int{4, 3, 4, 3, 2}

	for j := range expected2 {
		if actual2[j] != expected2[j] {
			t.Errorf("error: [id->%d] actual [%d] is not equal to expected [%d]\n", j, actual2[j], expected2[j])
		}
		init[j].Reset()
	}
}

func TestSeriesInsert(t *testing.T) {

	ctx := context.Background()

	tRef := time.Date(2017, 1, 1, 5, 30, 12, 0, time.UTC)

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", nil, 1.56),
		NewSeriesInt64("test", nil, 3, nil),
		NewSeriesString("test", nil, nil, "one"),
		NewSeriesMixed("test", nil, "three", 2.7),
		NewSeriesTime("test", nil, tRef, tRef.Add(24*time.Hour)),
	}

	// Insert more values to series
	for i := range init {
		s := init[i]
		switch st := s.(type) {
		case *SeriesInt64:
			st.Insert(s.NRows(), []int64{35, 12})
			st.Insert(s.NRows(), []*int64{&[]int64{14}[0], nil})

		case *SeriesFloat64:
			st.Insert(s.NRows(), []float64{34.6, 26.34, math.NaN()})

		case *SeriesString:
			st.Insert(s.NRows(), []string{"3", "no"})
			st.Insert(s.NRows(), []*string{&[]string{"4"}[0], nil})

		case *SeriesTime:
			st.Insert(s.NRows(), []time.Time{tRef.Add(4 * 24 * time.Hour), tRef.Add(3 * 24 * time.Hour)})
			st.Insert(s.NRows(), []*time.Time{&[]time.Time{tRef.Add(5 * 24 * time.Hour)}[0], nil})

		case *SeriesMixed:
			st.Insert(s.NRows(), []interface{}{53.5, "val", nil})
		}

	}

	expected := []Series{
		NewSeriesFloat64("test", nil, 1.56, 34.6, 26.34, nil),
		NewSeriesInt64("test", nil, 3, nil, 35, 12, 14, nil),
		NewSeriesString("test", nil, nil, "one", "3", "no", "4", nil),
		NewSeriesMixed("test", nil, "three", 2.7, 53.5, "val", nil),
		NewSeriesTime("test", nil, tRef, tRef.Add(24*time.Hour), tRef.Add(4*24*time.Hour), tRef.Add(3*24*time.Hour), tRef.Add(5*24*time.Hour), nil),
	}

	(expected[3].(*SeriesMixed)).SetIsEqualFunc(func(a, b interface{}) bool {
		g1 := a
		g2 := b

		if a == nil {
			if b == nil {
				return true
			}
			return false
		}
		if b == nil {
			return false
		}

		switch gTyp := a.(type) {
		case int:
			return g1.(int) == g2.(int)
		case int64:
			return g1.(int64) == g2.(int64)
		case float64:
			return g1.(float64) == g2.(float64)
		case string:
			return g1.(string) == g2.(string)
		default:
			t.Errorf("error: unknown type [%T]. yet to be added.", gTyp)

		}
		return false
	})

	for j := range expected {
		eq, err := expected[j].IsEqual(ctx, init[j])
		if err != nil {
			t.Errorf("error encountered: %s\n", err)
		}

		if !eq {
			spew.Dump(expected[j], init[j])
			t.Errorf("error: [id: %d] - expected: \n%s\n not equal to actual: \n%s\n", j, expected[j], init[j])
		}
	}
}

func TestSeriesValuesIterator(t *testing.T) {

	tRef := time.Date(2017, 1, 1, 5, 30, 12, 0, time.UTC)

	init := []Series{
		NewSeriesFloat64("test", nil, 1.56, 4.65, 56.34),
		NewSeriesInt64("test", nil, 3, 9, 9),
		NewSeriesString("test", nil, "strong", "one"),
		NewSeriesMixed("test", nil, "three", 2.7, 9),
		NewSeriesTime("test", nil, tRef, tRef.Add(24*time.Hour), tRef.Add(3*24*time.Hour)),
	}

	for i := range init {
		iterator := init[i].ValuesIterator()

		for {
			row, val, _ := iterator()

			if row == nil {
				break
			}
			v := init[i].Value(*row, DontLock)
			if !cmp.Equal(val, v) {
				t.Errorf("wrong val: expected: %T %v actual: %T %v", val, val, v, v)
			}

		}
	}
}
