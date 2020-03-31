// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

func TestNRows(t *testing.T) {

	s1 := NewSeriesInt64("day", nil, 1, 2, 3)
	s2 := NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2)
	df := NewDataFrame(s1, s2)

	expected := 3

	if df.NRows() != expected {
		t.Errorf("wrong val: expected: %v actual: %v", expected, df.NRows())
	}

}

func TestInsertAndRemove(t *testing.T) {

	s1 := NewSeriesInt64("day", nil, 1, 2, 3)
	s2 := NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2)
	df := NewDataFrame(s1, s2)

	df.Append(&dontLock, 9, 123.6)

	df.Append(&dontLock, map[string]interface{}{
		"day":   10,
		"sales": nil,
	})

	df.Remove(0)

	df.Prepend(&dontLock, map[string]interface{}{
		"day":   99,
		"sales": 199.99,
	})

	df.Prepend(&dontLock, 1000, 10000)
	df.UpdateRow(0, &dontLock, 10000, 1000)
	df.Update(0, 1, 9000)

	expected := `+-----+-------+---------+
|     |  DAY  |  SALES  |
+-----+-------+---------+
| 0:  | 10000 |  9000   |
| 1:  |  99   | 199.99  |
| 2:  |   2   |  23.4   |
| 3:  |   3   |  56.2   |
| 4:  |   9   |  123.6  |
| 5:  |  10   |   NaN   |
+-----+-------+---------+
| 6X2 | INT64 | FLOAT64 |
+-----+-------+---------+`

	if strings.TrimSpace(df.Table()) != strings.TrimSpace(expected) {
		t.Errorf("wrong val: expected: %v actual: %v", expected, df.Table())
	}
}

func TestSwap(t *testing.T) {

	s1 := NewSeriesInt64("day", nil, 1, 2, 3)
	s2 := NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2)
	df := NewDataFrame(s1, s2)

	df.Swap(0, 2)

	expectedValues := [][]interface{}{
		{int64(3), int64(2), int64(1)},
		{56.2, 23.4, 50.3},
	}

	iterator := df.ValuesIterator(ValuesOptions{0, 1, true})
	df.Lock()
	for {
		row, vals, _ := iterator()
		if row == nil {
			break
		}

		for key, val := range vals {
			switch idx := key.(type) {
			case int:
				expected := expectedValues[idx][*row]
				actual := val //df.Series[idx].Value(*row)

				if !cmp.Equal(expected, actual, cmpopts.IgnoreUnexported(SeriesFloat64{}, SeriesInt64{}, SeriesString{}, SeriesTime{}, SeriesGeneric{})) {
					t.Errorf("wrong val: expected: %T %v actual: %T %v", expected, expected, actual, actual)
				}
			}
		}
	}
	df.Unlock()
}

func TestNames(t *testing.T) {

	s1 := NewSeriesInt64("day", nil, 1, 2, 3)
	s2 := NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2)
	df := NewDataFrame(s1, s2)

	// Test names list
	expected := []string{"day", "sales"}

	actual := df.Names()

	if !cmp.Equal(expected, actual, cmpopts.IgnoreUnexported(SeriesFloat64{}, SeriesInt64{}, SeriesString{}, SeriesTime{}, SeriesGeneric{})) {
		t.Errorf("wrong val: expected: %T %v actual: %T %v", expected, expected, actual, actual)
	}

	// Test name to column
	input := []string{
		"day",
		"sales",
	}

	actuals := []int{
		0,
		1,
	}

	for i, colName := range input {

		actual, err := df.NameToColumn(colName)
		if err != nil {
			t.Errorf("wrong val: %s err: %v", colName, err)
		} else {
			expected := actuals[i]
			if !cmp.Equal(expected, actual, cmpopts.IgnoreUnexported(SeriesFloat64{}, SeriesInt64{}, SeriesString{}, SeriesTime{}, SeriesGeneric{})) {
				t.Errorf("wrong val: expected: %T %v actual: %T %v", expected, expected, actual, actual)
			}
		}
	}

	_, err := df.NameToColumn("unknown")
	if err == nil {
		t.Errorf("there should be an error when name is set to 'unknown'")
	}

}

func TestCopy(t *testing.T) {

	s1 := NewSeriesInt64("day", nil, 1, 2, 3)
	s2 := NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2)
	df := NewDataFrame(s1, s2)

	cp := df.Copy()

	if !cmp.Equal(df, cp, cmpopts.IgnoreUnexported(DataFrame{}, SeriesFloat64{}, SeriesInt64{}, SeriesString{}, SeriesTime{}, SeriesGeneric{})) {
		t.Errorf("wrong val: expected: %v actual: %v", df, cp)
	}
}

func TestSort(t *testing.T) {

	s1 := NewSeriesInt64("day", nil, nil, 1, 2, 4, 3, nil)
	s2 := NewSeriesFloat64("sales", nil, nil, 50.3, 23.4, 23.4, 56.2, nil)
	df := NewDataFrame(s1, s2)

	sks := []SortKey{
		{Key: "sales", Desc: true},
		{Key: "day", Desc: false},
	}

	df.Sort(context.Background(), sks)

	expectedValues := [][]interface{}{
		{int64(3), int64(1), int64(2), int64(4), nil, nil},
		{56.2, 50.3, 23.4, 23.4, nil, nil},
	}

	iterator := df.ValuesIterator(ValuesOptions{0, 1, true})
	df.Lock()
	for {
		row, vals, _ := iterator()
		if row == nil {
			break
		}

		for key, val := range vals {
			switch colName := key.(type) {
			case string:
				idx, _ := df.NameToColumn(colName, dontLock)

				expected := expectedValues[idx][*row]
				actual := val //df.Series[idx].Value(*row)

				if !cmp.Equal(expected, actual, cmpopts.IgnoreUnexported(SeriesFloat64{}, SeriesInt64{}, SeriesString{}, SeriesTime{}, SeriesGeneric{})) {
					t.Errorf("wrong val: expected: %T %v actual: %T %v", expected, expected, actual, actual)
				}
			}
		}
	}
	df.Unlock()

}

func TestDfIsEqual(t *testing.T) {
	ctx := context.Background()

	s1 := NewSeriesInt64("day", nil, nil, 1, 2, 4, 3, nil)
	s2 := NewSeriesFloat64("sales", nil, nil, 50.3, 23.4, 23.4, 56.2, nil)

	df1 := NewDataFrame(s1, s2)
	df2 := NewDataFrame(s1, s2)

	eq, err := df1.IsEqual(ctx, df2, IsEqualOptions{CheckName: true})
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("Df1: [%T] %s is not equal to Df2: [%T] %s\n", df1, df1.String(), df2, df2.String())
	}
}

func TestApplyDataFrameFn(t *testing.T) {
	ctx := context.Background()

	s1 := NewSeriesInt64("day", nil, 1, 2, 3)
	s2 := NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2)
	df := NewDataFrame(s1, s2)

	applyFn := ApplyDataFrameFn(func(vals map[interface{}]interface{}, row, nRows int) map[interface{}]interface{} {
		return map[interface{}]interface{}{
			"day":   vals["day"].(int64) * 3,
			"sales": vals["sales"].(float64) * 2,
		}
	})

	s3 := NewSeriesFloat64("expected day", nil, 3, 6, 9)
	s4 := NewSeriesFloat64("expected sales", nil, 100.6, 46.8, 112.4)

	expected := NewDataFrame(s3, s4)

	actual, err := Apply(ctx, df, applyFn, FilterOptions{InPlace: false})
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	eq, err := expected.IsEqual(ctx, actual.(*DataFrame))
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("actual result %s is not equal to expected result %s\n", actual.(*DataFrame).String(), expected.String())
	}
}

func TestDfClearAndUpdateRow(t *testing.T) {
	ctx := context.Background()

	s1 := NewSeriesInt64("day", nil, 1, 2, 3, 4)
	s2 := NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2, 33.6)
	df := NewDataFrame(s1, s2)

	// test RowClear
	df.ClearRow(3, DontLock) // clear row 4 to nil

	// Fetch and Swap row 3 with row 2
	row3 := df.Row(2, true)
	row2 := df.Row(1, true)

	// test UpdateRow
	df.UpdateRow(2, &DontLock, map[string]interface{}{
		"day":   row2["day"],
		"sales": row2["sales"],
	})

	df.UpdateRow(1, &DontLock, map[interface{}]interface{}{
		"day":   row3["day"],
		"sales": row3["sales"],
	})

	df.UpdateRow(0, &DontLock, map[interface{}]interface{}{
		0: 4,
		1: 33.6,
	})

	s3 := NewSeriesInt64("day", nil, 4, 3, 2, nil)
	s4 := NewSeriesFloat64("sales", nil, 33.6, 56.2, 23.4, nil)
	expected := NewDataFrame(s3, s4)

	eq, err := df.IsEqual(ctx, expected)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("actual result %s is not equal to expected result %s\n", df.String(), expected.String())
	}
}

func TestReorderColumnAddRemoveSeries(t *testing.T) {
	ctx := context.Background()

	// Reorder Columns
	s1 := NewSeriesInt64("day", nil, 1, 2, 3, 4)
	s2 := NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2, 33.6)
	s3 := NewSeriesFloat64("quantity", nil, 3, 6, 9, 7)

	df := NewDataFrame(s2, s3, s1)

	err := df.ReorderColumns([]string{"day", "sales", "quantity"}, DontLock)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	// Remove Series
	err = df.RemoveSeries("quantity", DontLock)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	// Add Series
	s4 := NewSeriesFloat64("volume", nil, 100.6, 46.8, 112.4, 97.34)

	err = df.AddSeries(s4, &[]int{2}[0], DontLock)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	expected := NewDataFrame(s1, s2, s4)

	eq, err := df.IsEqual(ctx, expected, IsEqualOptions{CheckName: true})
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("actual result %s is not equal to expected result %s\n", df.String(), expected.String())
	}

}

func TestDataFrameString(t *testing.T) {

	s1 := NewSeriesInt64("day", nil, 1, 2, 3, 4, 5, 6, 7)
	s2 := NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2, 23.4, 56.3, 37.56, 99.09)
	df := NewDataFrame(s1, s2)

	// fmt.Println(df.String())

	expected := `+-----+-------+---------+
|     |  DAY  |  SALES  |
+-----+-------+---------+
| 0:  |   1   |  50.3   |
| 1:  |   2   |  23.4   |
| 2:  |   3   |  56.2   |
|  ⋮  |   ⋮   |    ⋮    |
| 4:  |   5   |  56.3   |
| 5:  |   6   |  37.56  |
| 6:  |   7   |  99.09  |
+-----+-------+---------+
| 7X2 | INT64 | FLOAT64 |
+-----+-------+---------+`

	if strings.TrimSpace(df.String()) != strings.TrimSpace(expected) {
		t.Errorf("wrong val: expected:\n%v\n actual:\n%v\n", expected, df.String())
	}
}

func TestDataFrameFilter(t *testing.T) {
	ctx := context.Background()

	s1 := NewSeriesInt64("day", nil, 1, 2, 3, 4, 5, 6, 7)
	s2 := NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2, 23.4, 56.3, 37.56, 99.09)
	df := NewDataFrame(s1, s2)

	filterFn := FilterDataFrameFn(func(vals map[interface{}]interface{}, row, nRows int) (FilterAction, error) {
		sale := vals["sales"]
		if sale.(float64) <= 50.3 {
			return DROP, nil
		}
		return KEEP, nil
	})

	// Test Inplace: false
	higherSales, err := Filter(ctx, df, filterFn, FilterOptions{InPlace: false})
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	// Test Inplace: true
	_, err = Filter(ctx, df, filterFn, FilterOptions{InPlace: true})
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	actual := []*DataFrame{df, higherSales.(*DataFrame)}

	expected := `+-----+-------+---------+
|     |  DAY  |  SALES  |
+-----+-------+---------+
| 0:  |   3   |  56.2   |
| 1:  |   5   |  56.3   |
| 2:  |   7   |  99.09  |
+-----+-------+---------+
| 3X2 | INT64 | FLOAT64 |
+-----+-------+---------+`

	for _, dc := range actual {
		if strings.TrimSpace(dc.String()) != strings.TrimSpace(expected) {
			t.Errorf("wrong val: expected:\n%v\n actual:\n%v\n", expected, dc.String())
		}
	}

}

func TestDataFrameFillRand(t *testing.T) {

	ctx := context.Background()

	tRef := time.Date(2017, 1, 1, 5, 30, 12, 0, time.UTC)

	src := rand.NewSource(uint64(tRef.UTC().UnixNano()))
	uniform := distuv.Uniform{Min: 0, Max: 10000, Src: src}

	s1 := NewSeriesFloat64("testCol1", &SeriesInit{3, 0})
	s2 := NewSeriesInt64("testCol2", &SeriesInit{3, 0})
	s3 := NewSeriesString("testCol3", &SeriesInit{3, 0})

	df := NewDataFrame(s1, s2, s3)

	df.FillRand(src, 0.2, uniform)

	e1 := NewSeriesFloat64("expectedCol1", nil, 2229.633588925457, nil, 9917.578319979672)
	e2 := NewSeriesInt64("expectedCol2", nil, 7888, 8895, 6726)
	e3 := NewSeriesString("expectedCol3", nil, nil, "tTibWvLBYwWR", nil)
	eDf := NewDataFrame(e1, e2, e3)

	eq, err := eDf.IsEqual(ctx, df)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("actual DataFrame:\n%s\n not equal to expected DataFrame:\n%s\n", df.String(), eDf.String())
	}
}

func TestDataFrameValuesIterator(t *testing.T) {

	s1 := NewSeriesFloat64("price", nil, 1.56, 4.65, 56.34)
	s2 := NewSeriesInt64("quantity", nil, 3, 9, 9)
	s3 := NewSeriesString("title", nil, "strong", "milk", "custard")

	df := NewDataFrame(s1, s2, s3)
	iterator := df.ValuesIterator()

	for {
		row, vals, _ := iterator()
		if row == nil {
			break
		}

		v := df.Row(*row, true)
		if !cmp.Equal(vals, v) {
			t.Errorf("wrong val: expected: %T %v actual: %T %v", vals, vals, v, v)
		}

	}
}
