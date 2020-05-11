package funcs

import (
	"context"
	"fmt"
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

var ctx = context.Background()

func TestApplyFunction(t *testing.T) {

	s1 := dataframe.NewSeriesFloat64("x", nil, 1, 2, 3, 4, 5, 6)
	s2 := dataframe.NewSeriesFloat64("y", nil, 7, 8, 9, 10, 11, 12)
	s3 := dataframe.NewSeriesFloat64("z", nil, nil, nil, nil, nil, nil, nil)
	df := dataframe.NewDataFrame(s1, s2, s3)

	fn := []Func{
		{
			Fn:     "sin(x)+2*y",
			Domain: &[]dataframe.Range{dataframe.RangeFinite(0, 2)}[0],
		},
		{
			Fn:     "0",
			Domain: nil,
		},
	}

	err := ApplyFunction(ctx, df, fn)
	if err != nil {
		t.Errorf("wrong err: expected: %v got: %v", nil, err)
	}

	fmt.Print(df.Table())
}
