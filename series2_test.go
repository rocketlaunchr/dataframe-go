package dataframe

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/davecgh/go-spew/spew"
	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

func TestToSeriesString(t *testing.T) {
	ctx := context.Background()

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{1, 0}, 1, nil, 2, 3),
		NewSeriesInt64("test", &SeriesInit{1, 0}, 1, nil, 2, 3),
		NewSeriesMixed("test", &SeriesInit{1, 0}, 1, nil, 2, 3),
	}

	for _, s := range init {
		switch sm := s.(type) {

		case *SeriesFloat64:
			ss, err := sm.ToSeriesString(ctx, false)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			if ss.Type() != "string" {
				t.Errorf("actual type: %T is not convertible to expected type: *SeriesString\n", ss)
			}
		case *SeriesInt64:
			ss, err := sm.ToSeriesString(ctx, false)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			if ss.Type() != "string" {
				t.Errorf("actual type: %T is not convertible to expected type: *SeriesString\n", ss)
			}
		case *SeriesMixed:
			ss, err := sm.ToSeriesString(ctx, false)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			if ss.Type() != "string" {
				t.Errorf("actual type: %T is not convertible to expected type: *SeriesString\n", ss)
			}
		}

	}

}

func TestToSeriesMixed(t *testing.T) {
	ctx := context.Background()

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{1, 0}, 1, nil, 2, 3),
		NewSeriesInt64("test", &SeriesInit{1, 0}, 1, nil, 2, 3),
		NewSeriesString("test", &SeriesInit{1, 0}, "1", nil, "2", "3"),
		NewSeriesTime("test", &SeriesInit{1, 0}, time.Now(), nil, time.Now(), time.Now()),
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, nil, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}),
	}

	for _, s := range init {

		switch sm := s.(type) {

		case *SeriesFloat64:
			ss, err := sm.ToSeriesMixed(ctx, false)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			if ss.Type() != "mixed" {
				t.Errorf("actual type: %T is not convertible to expected type: *SeriesMixed\n", ss)
			}
		case *SeriesInt64:
			ss, err := sm.ToSeriesMixed(ctx, false)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			if ss.Type() != "mixed" {
				t.Errorf("actual type: %T is not convertible to expected type: *SeriesMixed\n", ss)
			}
		case *SeriesString:
			ss, err := sm.ToSeriesMixed(ctx, false)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			if ss.Type() != "mixed" {
				t.Errorf("actual type: %T is not convertible to expected type: *SeriesMixed\n", ss)
			}
		case *SeriesTime:
			ss, err := sm.ToSeriesMixed(ctx, false)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			if ss.Type() != "mixed" {
				t.Errorf("actual type: %T is not convertible to expected type: *SeriesMixed\n", ss)
			}
		case *SeriesGeneric:
			ss, err := sm.ToSeriesMixed(ctx, false)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			if ss.Type() != "mixed" {
				t.Errorf("actual type: %T is not convertible to expected type: *SeriesMixed\n", ss)
			}
		}

	}

}

func TestToSeriesFloat64(t *testing.T) {
	ctx := context.Background()

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{1, 0}, 1, nil, 2, 3),
		NewSeriesInt64("test", &SeriesInit{1, 0}, 1, nil, 2, 3),
		NewSeriesString("test", &SeriesInit{1, 0}, "1", nil, "2", "3"),
		NewSeriesTime("test", &SeriesInit{1, 0}, time.Now(), nil, time.Now(), time.Now()),
	}

	for _, s := range init {

		switch sm := s.(type) {

		case *SeriesInt64:
			ss, err := sm.ToSeriesFloat64(ctx, false)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			if ss.Type() != "float64" {
				t.Errorf("actual type: %T is not convertible to expected type: *SeriesFloat64\n", ss)
			}
		case *SeriesFloat64:
			ss, err := sm.ToSeriesFloat64(ctx, true)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			if ss.Type() != "float64" {
				t.Errorf("actual type: %T is not convertible to expected type: *SeriesFloat64\n", ss)
			}
		case *SeriesString:
			ss, err := sm.ToSeriesFloat64(ctx, false)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			if ss.Type() != "float64" {
				t.Errorf("actual type: %T is not convertible to expected type: *SeriesFloat64\n", ss)
			}
		case *SeriesTime:
			ss, err := sm.ToSeriesFloat64(ctx, false)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			if ss.Type() != "float64" {
				t.Errorf("actual type: %T is not convertible to expected type: *SeriesFloat64\n", ss)
			}
		}

	}

}

func TestToSeriesInt64(t *testing.T) {
	ctx := context.Background()

	// Create new series
	init := []Series{
		NewSeriesString("test", &SeriesInit{1, 0}, "1", nil, "2", "3"),
		NewSeriesTime("test", &SeriesInit{1, 0}, time.Now(), nil, time.Now(), time.Now()),
	}

	for _, s := range init {

		switch sm := s.(type) {

		case *SeriesString:
			ss, err := sm.ToSeriesInt64(ctx, false)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			if ss.Type() != "int64" {
				t.Errorf("actual type: %T is not convertible to expected type: *SeriesInt64\n", ss)
			}
		case *SeriesTime:
			ss, err := sm.ToSeriesInt64(ctx, false)
			if err != nil {
				t.Errorf("error encountered: %s\n", err)
			}
			if ss.Type() != "int64" {
				t.Errorf("actual type: %T is not convertible to expected type: *SeriesInt64\n", ss)
			}
		}

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
		NewSeriesMixed("test", &SeriesInit{1, 0}, 1, "two", 3.0),
		NewSeriesGeneric("test", civil.Date{}, &SeriesInit{0, 1}, civil.Date{2018, time.May, 01}, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03}),
	}

	(init[5].(*SeriesGeneric)).SetIsEqualFunc(func(a, b interface{}) bool {
		g1 := a.(civil.Date)
		g2 := b.(civil.Date)
		return g1 == g2
	})

	(init[4].(*SeriesMixed)).SetIsEqualFunc(func(a, b interface{}) bool {
		g1 := a
		g2 := b

		switch gTyp := g1.(type) {
		case int64, int:
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

	expected := []Series{
		NewSeriesFloat64("expected", &SeriesInit{1, 0}, 1.0, 2.0, 3.0),
		NewSeriesInt64("expected", &SeriesInit{1, 0}, 1, 2, 3),
		NewSeriesString("expected", &SeriesInit{1, 0}, "1", "2", "3"),
		NewSeriesTime("expected", &SeriesInit{1, 0}, []time.Time{tRef, tRef.Add(24 * time.Hour), tRef.Add(2 * 24 * time.Hour)}),
		NewSeriesMixed("expected", &SeriesInit{1, 0}, 1, "two", 3.0),
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

func TestApplySeriesFn(t *testing.T) {
	ctx := context.Background()

	s := NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2)

	applyFn := ApplySeriesFn(func(val interface{}, row, nRows int) interface{} {
		return val.(float64) + 10
	})
	expected := NewSeriesFloat64("expected", nil, 60.3, 33.4, 66.2)

	actual, err := Apply(ctx, s, applyFn, FilterOptions{InPlace: false})
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	eq, err := expected.IsEqual(ctx, actual.(Series))
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("actual result %v is not equal to expected result %v\n", actual.(SeriesFloat64), expected)
	}

}

func TestSeriesFillRand(t *testing.T) {

	ctx := context.Background()

	tRef := time.Date(2017, 1, 1, 5, 30, 12, 0, time.UTC)

	src := rand.NewSource(uint64(tRef.UTC().UnixNano()))
	uniform := distuv.Uniform{Min: 0, Max: 10000, Src: src}

	// Create new series
	init := []Series{
		NewSeriesFloat64("test", &SeriesInit{3, 0}),
		NewSeriesInt64("test", &SeriesInit{3, 0}),
		NewSeriesString("test", &SeriesInit{3, 0}),
		NewSeriesMixed("test", &SeriesInit{3, 0}),

		NewSeriesFloat64("test", &SeriesInit{3, 5}),
		NewSeriesInt64("test", &SeriesInit{3, 5}),
		NewSeriesString("test", &SeriesInit{3, 5}),
		NewSeriesMixed("test", &SeriesInit{3, 5}),

		NewSeriesTime("test", &SeriesInit{3, 0}),
		NewSeriesTime("test", &SeriesInit{3, 5}),
	}

	for i := range init {
		s, ok := init[i].(FillRander)
		if !ok {
			t.Errorf("error: %T is not a FillRander", init[i])
		}
		s.FillRand(src, 0.2, uniform)
	}

	expected := []Series{
		NewSeriesFloat64("test", nil, 2229.633588925457, nil, 9917.578319979672),
		NewSeriesInt64("test", nil, 7888, 8895, 6726),
		NewSeriesString("test", nil, nil, "tTibWvLBYwWR", nil),
		NewSeriesMixed("test", nil, 6866.071268830753, 4018.6214545707035, 1463.8310850524738),

		NewSeriesFloat64("test", nil, 6196.436230930121, 4685.770139578198, nil, 1814.3621211065197, 2971.275110795939),
		NewSeriesInt64("test", nil, 2735, 908, nil, 7888, 9911),
		NewSeriesString("test", nil, "1hZQJPsdg8EC", "WgQx2fxJ2LcO", nil, nil, "QaQEHA1sZkal"),
		NewSeriesMixed("test", nil, nil, 7162.861880703196, 6196.960175399804, 1082.8867644214768, 1174.108927948494),
	}

	for j := range expected {
		exp := expected[j]

		eq, err := exp.IsEqual(ctx, init[j])
		if err != nil {
			t.Errorf("error encountered: %s\n", err)
		}

		if !eq {
			spew.Dump(init[j])
			t.Errorf("line [%d] -> actual series:\n%v\n not equal to expected series:\n%v\n", j, init[j], expected[j])
		}
	}
}
