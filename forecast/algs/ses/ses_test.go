// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package ses_test

import (
	"context"
	"math"
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	. "github.com/rocketlaunchr/dataframe-go/forecast/algs/ses"
)

var ctx = context.Background()

func TestSES(t *testing.T) {

	// Test data from https://www.itl.nist.gov/div898/handbook/pmc/section4/pmc431.htm
	data12 := dataframe.NewSeriesFloat64("data", nil, 71, 70, 69, 68, 64, 65, 72, 78, 75, 75, 75, 70)

	alg := NewExponentialSmoothing()
	cfg := ExponentialSmoothingConfig{Alpha: 0.1}

	err := alg.Configure(cfg)
	if err != nil {
		t.Fatalf("configure error: %v", err)
	}

	err = alg.Load(ctx, data12, nil)
	if err != nil {
		t.Fatalf("load error: %v", err)
	}

	pred, _, err := alg.Predict(ctx, 5)
	if err != nil {
		t.Fatalf("pred error: %v", err)
	}

	// Expected values from https://www.itl.nist.gov/div898/handbook/pmc/section4/pmc432.htm
	expPred := dataframe.NewSeriesFloat64("expected", nil, 71.50, 71.35, 71.21, 71.09, 70.98)

	// compared expPred with pred
	iterator := pred.ValuesIterator(dataframe.ValuesOptions{Step: 1, DontReadLock: true}) // func() (*int, interface{}, int)

	for {
		row, val, _ := iterator()
		if row == nil {
			break
		}

		roundedPred := math.Round(val.(float64)*100) / 100
		if roundedPred != expPred.Values[*row] {
			t.Fatalf("forecasting error. expected = %v, actual = %v (rounded: %v)", expPred.Values[*row], val.(float64), roundedPred)
		}
	}
}
