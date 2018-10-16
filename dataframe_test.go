package dataframe

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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

	df.Append(9, 123.6)

	df.Append(map[string]interface{}{
		"day":   10,
		"sales": nil,
	})

	df.Remove(0)

	df.Prepend(map[string]interface{}{
		"day":   99,
		"sales": 199.99,
	})

	df.Prepend(1000, 10000)
	df.UpdateRow(0, 10000, 1000)
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

	iterator := df.Values(ValuesOptions{0, 1, true})
	df.Lock()
	for {
		row, vals := iterator()
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
		{Key: "sales", SortDesc: true},
		{Key: "day", SortDesc: false},
	}

	df.Sort(sks)

	expectedValues := [][]interface{}{
		{int64(3), int64(1), int64(2), int64(4), nil, nil},
		{56.2, 50.3, 23.4, 23.4, nil, nil},
	}

	iterator := df.Values(ValuesOptions{0, 1, true})
	df.Lock()
	for {
		row, vals := iterator()
		if row == nil {
			break
		}

		for key, val := range vals {
			switch colName := key.(type) {
			case string:
				idx, _ := df.NameToColumn(colName)

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
