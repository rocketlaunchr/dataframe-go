package hw

import (
	"context"
	"fmt"
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	evalFn "github.com/rocketlaunchr/dataframe-go/forecast/evaluation"
)

func TestHW(t *testing.T) {
	ctx := context.Background()

	// 48 + 24 = 72 data pts + extra 12
	data := dataframe.NewSeriesFloat64("simple data", nil, 30, 21, 29, 31, 40, 48, 53, 47, 37, 39, 31, 29, 17, 9, 20, 24, 27, 35, 41, 38,
		27, 31, 27, 26, 21, 13, 21, 18, 33, 35, 40, 36, 22, 24, 21, 20, 17, 14, 17, 19,
		26, 29, 40, 31, 20, 24, 18, 26, 17, 9, 17, 21, 28, 32, 46, 33, 23, 28, 22, 27,
		18, 8, 17, 21, 31, 34, 44, 38, 31, 30, 26, 32, 45, 34, 30, 27, 25, 22, 28, 33, 42, 32, 40, 52)

	var (
		period int  = 12
		h      uint = 24
	)

	// fmt.Println(data.Table())
	alpha := 0.45
	beta := 0.03
	gamma := 0.73

	cfg := HoltWintersConfig{
		Alpha:    alpha,
		Beta:     beta,
		Gamma:    gamma,
		Period:   period,
		Seasonal: ADD,
		Trend:    ADD,
	}

	hwModel := NewHoltWinters()

	if err := hwModel.Configure(cfg); err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if err := hwModel.Load(ctx, data, &dataframe.Range{End: &[]int{71}[0]}); err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	hwPredict, err := hwModel.Predict(ctx, h)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	fmt.Println(hwPredict.Table())

	errVal, err := hwModel.Evaluate(ctx, hwPredict, evalFn.RootMeanSquaredError)
	if err != nil {
		t.Errorf("error encountered: %s", err)
	}
	fmt.Println("Root Mean Squared Error", errVal)
}
