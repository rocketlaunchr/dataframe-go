// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package ets

import (
	"context"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/rocketlaunchr/dataframe-go/forecast"
)

// Evaluate will measure the quality of the predicted values based on the evaluation calculation defined by evalFunc.
// It will compare the error between sf and the values from the end of the loaded data ("validation set").
// sf is usually the output of the Predict method.
//
// NOTE: You can use the functions directly from the validation subpackage if you need to do something
// other than that described above.
func (es *HoltWinters) Evaluate(ctx context.Context, sf *dataframe.SeriesFloat64, evalFunc forecast.EvaluationFunc) (float64, error) {

	if evalFunc == nil {
		panic("evalFunc is nil")
	}

	// Determine outer range of loaded data
	loadedSeries := es.sf
	loadedRows := loadedSeries.NRows(dataframe.DontLock)

	_, te, err := es.tRange.Limits(loadedRows)
	if err != nil {
		return 0, err
	}

	s, e, err := (&dataframe.Range{Start: &[]int{te + 1}[0]}).Limits(loadedRows)
	if err != nil {
		// There is no data in validation set
		return 0, nil
	}

	lengthOfValidationSet := e - s + 1
	lengthOfPredictionSet := sf.NRows(dataframe.DontLock)

	// Pick the smallest range
	var minR int
	if lengthOfValidationSet < lengthOfPredictionSet {
		minR = lengthOfValidationSet
	} else {
		minR = lengthOfPredictionSet
	}

	errVal, _, err := evalFunc(ctx, loadedSeries.Values[s:s+minR], sf.Values[0:minR], nil)
	return errVal, err
}
