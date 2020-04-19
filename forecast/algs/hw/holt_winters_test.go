package hw

import (
	"context"
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
		18, 8, 17, 21, 31, 34, 44, 38, 31, 30, 26, 32, 45, 34, 30, 27, 25, 22, 28, 33, 42, 32, 40, 52,
	)

	var (
		period int  = 12
		h      uint = 24
	)

	// fmt.Println(data.Table())
	alpha := 0.716
	beta := 0.029
	gamma := 0.993

	cfg := HoltWintersConfig{
		Alpha:    alpha,
		Beta:     beta,
		Gamma:    gamma,
		Period:   period,
		Seasonal: ADD,
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

	expected := dataframe.NewSeriesFloat64("expected", nil,
		26.27699081580312, 12.48133856351768, 22.077813893501844, 26.839391481818982, 31.600180075780813, 30.48212275836478,
		41.83687016138987, 44.46400992890207, 32.183690818478226, 27.244288540553175, 28.104940474424172, 33.28718444078283,
		25.754987449064725, 11.95933519677928, 21.555810526763448, 26.317388115080583, 31.078176709042417, 29.960119391626378,
		41.31486679465147, 43.94200656216367, 31.66168745173983, 26.722285173814775, 27.582937107685776, 32.76518107404443,
	)

	eq, err := hwPredict.IsEqual(ctx, expected)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	if !eq {
		t.Errorf("prection: \n%s\n is not equal to expected: \n%s\n", hwPredict.Table(), expected.Table())
	}

	errVal, err := hwModel.Evaluate(ctx, hwPredict, evalFn.RootMeanSquaredError)
	if err != nil {
		t.Errorf("error encountered: %s", err)
	}
	expRMSE := 12.666953719779478

	if errVal != expRMSE {
		t.Errorf("expected error calc Value: %f is not same as actual errVal: %f", expRMSE, errVal)
	}
}
