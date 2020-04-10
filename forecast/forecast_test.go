package forecast

// import (
// 	"context"
// 	"fmt"
// 	"testing"

// 	dataframe "github.com/rocketlaunchr/dataframe-go"
// 	alg "github.com/rocketlaunchr/dataframe-go/forecast/algs/ets"
// 	eval "github.com/rocketlaunchr/dataframe-go/forecast/evaluation"
// )

// func TestForecastSeries(t *testing.T) {
// 	ctx := context.Background()

// 	data := dataframe.NewSeriesFloat64("unit increase", nil, 1.05, 2.5, 3, 4, 5, 6, 7.25, 8, 9.36, 10.04)
// 	var m uint = 10

// 	var alpha float64 = 0.4

// 	cfg := alg.ExponentialSmoothingConfig{
// 		Alpha: alpha,
// 	}

// 	// var evalFn EvaluationFunc = eval.RootMeanSquaredError

// 	// res, evalErr, err := Forecast(ctx, data, &dataframe.Range{End: &[]int{5}[0]}, alg.ExponentialSmoothing, cfg, m, evalFn)
// 	// if err != nil {
// 	// 	t.Errorf("error encountered: %s\n", err)
// 	// }

// 	fmt.Println(res.(*dataframe.SeriesFloat64).Table())
// 	fmt.Println("RMSE: ", evalErr)
// }
