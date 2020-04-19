package ses

import (
	"context"
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	eval "github.com/rocketlaunchr/dataframe-go/forecast/evaluation"
)

func TestETS(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("unit increase", nil, 1.05, 2.5, 3, 4, 5, 6, 7.25, 8, 9.36, 10.04)
	var m uint = 10

	var alpha float64 = 0.4

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

	etsPredict, err := etsModel.Predict(ctx, m)
	if err != nil {
		t.Errorf("error encountered: %s", err)
	}

	expected := dataframe.NewSeriesFloat64("expected", nil,
		4.646448, 4.646448, 4.646448, 4.646448, 4.646448,
		4.646448, 4.646448, 4.646448, 4.646448, 4.646448)

	eq, err := etsPredict.IsEqual(ctx, expected)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	if !eq {
		t.Errorf("prection: \n%s\n is not equal to expected: \n%s\n", etsPredict.Table(), expected.Table())
	}

	evalFn := eval.RootMeanSquaredError

	errVal, err := etsModel.Evaluate(ctx, etsPredict, evalFn)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	expectedRmse := 4.1633150753580965

	if errVal != expectedRmse {
		t.Errorf("expected error calc Value: %f is not same as actual errVal: %f", expectedRmse, errVal)
	}

}
