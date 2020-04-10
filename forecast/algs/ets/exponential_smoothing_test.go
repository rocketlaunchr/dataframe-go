package ets

import (
	"context"
	"fmt"
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	eval "github.com/rocketlaunchr/dataframe-go/forecast/evaluation"
)

func TestETS(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("unit increase", nil, 1.05, 2.5, 3, 4, 5, 6, 7.25, 8, 9.36, 10.04)
	var m uint = 10

	var alpha float64 = 0.4

	// fmt.Println(data.Table())

	etsModel := NewExponentialSmoothing()

	cfg := ExponentialSmoothingConfig{
		Alpha: alpha,
	}

	if err := etsModel.Configure(cfg); err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if err := etsModel.Load(ctx, data, &dataframe.Range{End: &[]int{5}[0]}); err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	// spew.Dump(etsModel)

	etsPredict, err := etsModel.Predict(ctx, m)
	if err != nil {
		t.Errorf("error encountered: %s", err)
	}
	fmt.Println(etsPredict.Table())

	evalFn := eval.RootMeanSquaredError

	errVal, err := etsModel.Evaluate(ctx, etsPredict, evalFn)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	fmt.Println("Root Mean Squared Error", errVal)
}
