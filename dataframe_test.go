package dataframe

import (
	"strings"
	"testing"
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

// iterator := df.Values(dataframe.ValuesOptions{0, 1, true}) // Don't apply read lock because we are write locking from outside.

// df.Lock()
// for {
// 	row, vals := iterator()
// 	if row == nil {
// 		break
// 	}

// 	fmt.Println(*row, vals)

// }
// df.Unlock()
