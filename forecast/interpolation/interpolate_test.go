// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package forecast

import (
	"context"
	"testing"

	"github.com/rocketlaunchr/dataframe-go"
)

func TestInterpolateSeriesForwardFillFwd(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 50.3, nil, nil, 56.2, 45.34, nil, 39.26, nil)

	opts := InterpolateOptions{
		Method:        ForwardFill{},
		FillDirection: Forward,
		Limit:         &[]int{1}[0],
		FillRegion:    nil,
		InPlace:       true,
	}
	expected := dataframe.NewSeriesFloat64("expected", nil, 50.3, 50.3, 50.3, nil, 56.2, 45.34, 45.34, 39.26, 39.26)

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	eq, err := data.IsEqual(ctx, expected)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("[%T] %v Not Equal to [%T] %v", data, data.Values, expected, expected.Values)
	}
}

func TestInterpolateSeriesForwardFillBkwd(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 25.7, nil, nil, 36.6, 45.2, nil, 39.26, nil)

	opts := InterpolateOptions{
		Method:        ForwardFill{},
		FillDirection: Backward,
		FillRegion:    nil,
		InPlace:       true,
	}
	expected := dataframe.NewSeriesFloat64("expected", nil, 25.7, 25.7, 25.7, 25.7, 36.6, 45.2, 45.2, 39.26, 39.26)

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	eq, err := data.IsEqual(ctx, expected)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("[%T] %v Not Equal to [%T] %v", data, data.Values, expected, expected.Values)
	}
}

func TestInterpolateSeriesForwardFillBoth(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 50.3, nil, nil, 56.2, 45.34, nil, 39.26, nil)

	opts := InterpolateOptions{
		Method:        ForwardFill{},
		FillDirection: Forward | Backward,
		FillRegion:    nil,
		InPlace:       true,
	}
	expected := dataframe.NewSeriesFloat64("expected", nil, 50.3, 50.3, 50.3, 50.3, 56.2, 45.34, 45.34, 39.26, 39.26)

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	eq, err := data.IsEqual(ctx, expected)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("[%T] %v Not Equal to [%T] %v", data, data.Values, expected, expected.Values)
	}
}

func TestInterpolateSeriesBackwardFillBkwd(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 25.7, nil, nil, 36.6, 45.2, nil, 39.26, nil)

	opts := InterpolateOptions{
		Method:        BackwardFill{},
		FillDirection: Backward,
		FillRegion:    nil,
		InPlace:       true,
	}
	expected := dataframe.NewSeriesFloat64("expected", nil, 25.7, 25.7, 36.6, 36.6, 36.6, 45.2, 39.26, 39.26, 39.26)

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	eq, err := data.IsEqual(ctx, expected)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("[%T] %v Not Equal to [%T] %v", data, data.Values, expected, expected.Values)
	}
}

func TestInterpolateSeriesBackwardFillFwd(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 50.3, nil, nil, 56.2, 45.34, nil, 39.26, nil)

	opts := InterpolateOptions{
		Method:        BackwardFill{},
		FillDirection: Forward,
		FillRegion:    nil,
		InPlace:       true,
	}
	expected := dataframe.NewSeriesFloat64("expected", nil, 50.3, 50.3, 56.2, 56.2, 56.2, 45.34, 39.26, 39.26, 39.26)

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	eq, err := data.IsEqual(ctx, expected)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("[%T] %v Not Equal to [%T] %v", data, data.Values, expected, expected.Values)
	}
}

func TestInterpolateSeriesBackwardFillBoth(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 50.3, nil, nil, 56.2, 45.34, nil, 39.26, nil)

	opts := InterpolateOptions{
		Method:        BackwardFill{},
		FillDirection: Forward | Backward,
		FillRegion:    nil,
		InPlace:       true,
	}
	expected := dataframe.NewSeriesFloat64("expected", nil, 50.3, 50.3, 56.2, 56.2, 56.2, 45.34, 39.26, 39.26, 39.26)

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	eq, err := data.IsEqual(ctx, expected)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("[%T] %v Not Equal to [%T] %v", data, data.Values, expected, expected.Values)
	}
}

func TestInterpolateSeriesLinearFillFwd(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 29.33, nil, nil, nil, 21.7, 35.14, nil, nil, 50.66, nil)

	opts := InterpolateOptions{
		Method:        Linear{},
		FillDirection: Forward,
		FillRegion:    nil,
		InPlace:       true,
	}
	expected := dataframe.NewSeriesFloat64("expected", nil, 31.237499999999997, 29.33, 27.4225, 25.515, 23.6075, 21.7, 35.14, 40.31333333333333, 45.486666666666665, 50.66, 55.83333333333333)

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	eq, err := data.IsEqual(ctx, expected)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("[%T] %v Not Equal to [%T] %v", data, data.Values, expected, expected.Values)
	}
}

func TestInterpolateSeriesLinearFillBkwd(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 29.33, nil, nil, nil, 21.7, 35.14, nil, nil, 50.66, nil)

	opts := InterpolateOptions{
		Method:        Linear{},
		FillDirection: Backward,
		FillRegion:    nil,
		InPlace:       true,
	}
	expected := dataframe.NewSeriesFloat64("expected", nil, 31.237499999999997, 29.33, 27.4225, 25.515, 23.6075, 21.7, 35.14, 40.31333333333333, 45.486666666666665, 50.66, 55.83333333333333)
	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	eq, err := data.IsEqual(ctx, expected)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("[%T] %v Not Equal to [%T] %v", data, data.Values, expected, expected.Values)
	}
}

func TestInterpolateSeriesLinearFillBoth(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 29.33, nil, nil, nil, 21.7, 35.14, nil, nil, 50.66, nil)

	opts := InterpolateOptions{
		Method:        Linear{},
		FillDirection: Forward | Backward,
		FillRegion:    &[]FillRegion{Interpolation}[0],
		InPlace:       true,
	}
	expected := dataframe.NewSeriesFloat64("expected", nil, nil, 29.33, 27.4225, 23.6075, 25.515, 21.7, 35.14, 40.31333333333333, 45.486666666666665, 50.66, nil)

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	eq, err := data.IsEqual(ctx, expected)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if !eq {
		t.Errorf("[%T] %v Not Equal to [%T] %v", data, data.Values, expected, expected.Values)
	}
}

func TestInterpolateDfForwardFill(t *testing.T) {
	ctx := context.Background()

	s1 := dataframe.NewSeriesFloat64("column 1", nil, nil, 29.33, nil, nil, nil, 21.7, 35.14, nil, nil)
	s2 := dataframe.NewSeriesFloat64("column 2", nil, nil, 50.3, nil, nil, 56.2, 45.34, nil, 39.26, nil)

	df := dataframe.NewDataFrame(s1, s2)
	opts := InterpolateOptions{
		Method:        ForwardFill{},
		FillDirection: Forward,
		Limit:         nil,
		FillRegion:    nil,
		InPlace:       true,
	}

	s3 := dataframe.NewSeriesFloat64("column 3", nil, 29.33, 29.33, 29.33, 29.33, 29.33, 21.7, 35.14, 35.14, 35.14)
	s4 := dataframe.NewSeriesFloat64("column 4", nil, 50.3, 50.3, 50.3, 50.3, 56.2, 45.34, 45.34, 39.26, 39.26)

	expected := dataframe.NewDataFrame(s3, s4)

	_, err := Interpolate(ctx, df, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	eq, err := df.IsEqual(ctx, expected)
	if err != nil {
		t.Errorf("error encountered %s", err)
	}

	if !eq {
		t.Errorf("df: [%T]\n[%s]\n is not equal to expected: [%T]\n%s\n", df, df.String(), expected, expected.String())
	}
}
