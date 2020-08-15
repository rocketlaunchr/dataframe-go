package funcs

import (
	"context"
	"math"
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

var ctx = context.Background()

func TestEvaluate(t *testing.T) {

	s1 := dataframe.NewSeriesFloat64("x", nil, 1, 2, 3, 4, 5, 6)
	s2 := dataframe.NewSeriesFloat64("y", nil, 7, 8, 9, 10, 11, 12)
	s3 := dataframe.NewSeriesString("s", nil, "*7", "*8", "*9", "*10", "*11", "*12")
	s4 := dataframe.NewSeriesFloat64("z", nil, nil, nil, nil, nil, nil, nil)
	df := dataframe.NewDataFrame(s1, s2, s3, s4)

	fn := []SubFuncDefn{
		{
			Fn:     "sin(x)+2*y+add1()",
			Domain: &[]dataframe.Range{dataframe.RangeFinite(0, 2)}[0],
		},
		{
			Fn:     "0",
			Domain: nil,
		},
	}

	opts := EvaluateOptions{
		CustomFns: map[string]func(args ...float64) float64{
			"add1": func(args ...float64) float64 {
				return 1
			},
		},
		Range: &[]dataframe.Range{dataframe.RangeFinite(0, -2)}[0],
	}

	err := Evaluate(ctx, df, fn, 3, opts)
	if err != nil {
		panic(err)
	}

	// Compared with expected results
	sexp := dataframe.NewSeriesFloat64("z", nil,
		math.Sin(1)+2*7+1,
		math.Sin(2)+2*8+1,
		math.Sin(3)+2*9+1,
		0,
		0,
		math.NaN(),
	)

	expectedDf := dataframe.NewDataFrame(s1, s2, s3, sexp)
	eq, err := expectedDf.IsEqual(ctx, df)
	if err != nil {
		t.Errorf("wrong err: expected: %v got: %v", nil, err)
	}

	if !eq {
		t.Errorf("wrong err: expected: %v got: %v", expectedDf, df)
	}
}
