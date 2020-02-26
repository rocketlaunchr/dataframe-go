// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package forecast

import (
	"context"
	"fmt"
	"testing"

	"github.com/rocketlaunchr/dataframe-go"
)

func TestInterpolateSeriesForwardFillFwd(t *testing.T) {
	ctx := context.Background()

	fmt.Printf("\nInterpolate Series ForwardFill Fwd \n(Max consecutive NaN fill Limit => 1)...\n\n")

	data := dataframe.NewSeriesFloat64("values", nil, nil, 50.3, nil, nil, 56.2, 45.34, nil, 39.26, nil)

	fmt.Println("before:")
	fmt.Println(data.Values)

	opts := InterpolateOptions{
		Method:        ForwardFill,
		FillDirection: Forward,
		Limit:         &[]int{1}[0],
		FillRegion:    nil,
		InPlace:       true,
	}

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	fmt.Println("after:")
	fmt.Println(data.Values)
}

func TestInterpolateSeriesForwardFillBkwd(t *testing.T) {
	ctx := context.Background()
	fmt.Printf("\nInterpolate Series ForwardFill Bkwd...\n\n")

	data := dataframe.NewSeriesFloat64("values", nil, nil, 25.7, nil, nil, 36.6, 45.2, nil, 39.26, nil)
	fmt.Println("before:")
	fmt.Println(data.Values)

	opts := InterpolateOptions{
		Method:        ForwardFill,
		FillDirection: Backward,
		FillRegion:    nil,
		InPlace:       true,
	}

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	fmt.Println("after:")
	fmt.Println(data.Values)
}

func TestInterpolateSeriesForwardFillBoth(t *testing.T) {
	ctx := context.Background()

	fmt.Printf("\nInterpolate Series ForwardFill Both (Fwd and Bkwd) ...\n\n")

	data := dataframe.NewSeriesFloat64("values", nil, nil, 50.3, nil, nil, 56.2, 45.34, nil, 39.26, nil)
	fmt.Println("before:")
	fmt.Println(data.Values)

	opts := InterpolateOptions{
		Method:        ForwardFill,
		FillDirection: Forward | Backward,
		FillRegion:    nil,
		InPlace:       true,
	}

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	fmt.Println("after:")
	fmt.Println(data.Values)
}

func TestInterpolateSeriesBackwardFillBkwd(t *testing.T) {
	ctx := context.Background()
	fmt.Printf("\nInterpolate Series BackwardFill Bkwd...\n\n")

	data := dataframe.NewSeriesFloat64("values", nil, nil, 25.7, nil, nil, 36.6, 45.2, nil, 39.26, nil)
	fmt.Println("before:")
	fmt.Println(data.Values)

	opts := InterpolateOptions{
		Method:        BackwardFill,
		FillDirection: Backward,
		FillRegion:    nil,
		InPlace:       true,
	}

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	fmt.Println("after:")
	fmt.Println(data.Values)
}

func TestInterpolateSeriesBackwardFillFwd(t *testing.T) {
	ctx := context.Background()

	fmt.Printf("\nInterpolate Series BackwardFill Fwd...\n\n")

	data := dataframe.NewSeriesFloat64("values", nil, nil, 50.3, nil, nil, 56.2, 45.34, nil, 39.26, nil)
	fmt.Println("before:")
	fmt.Println(data.Values)

	opts := InterpolateOptions{
		Method:        BackwardFill,
		FillDirection: Forward,
		FillRegion:    nil,
		InPlace:       true,
	}

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	fmt.Println("after:")
	fmt.Println(data.Values)
}

func TestInterpolateSeriesBackwardFillBoth(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 50.3, nil, nil, 56.2, 45.34, nil, 39.26, nil)

	opts := InterpolateOptions{
		Method:        BackwardFill,
		FillDirection: Forward | Backward,
		FillRegion:    nil,
		InPlace:       true,
	}

	fmt.Printf("\nInterpolate Series BackwardFill Both (Fwd and Bkwd) ...\n\n")

	fmt.Println("before:")
	fmt.Println(data.Values)

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	fmt.Println("after:")
	fmt.Println(data.Values)
}

func TestInterpolateSeriesLinearFillFwd(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 29.33, nil, nil, nil, 21.7, 35.14, nil, nil, 50.66, nil)

	opts := InterpolateOptions{
		Method:        Linear,
		FillDirection: Forward,
		FillRegion:    nil,
		InPlace:       true,
	}

	fmt.Printf("\nInterpolate Series Linear Fwd...\n\n")
	fmt.Println("before:")
	fmt.Println(data.Values)

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	fmt.Println("after:")
	fmt.Println(data.Values)
}

func TestInterpolateSeriesLinearFillBkwd(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 29.33, nil, nil, nil, 21.7, 35.14, nil, nil, 50.66, nil)

	opts := InterpolateOptions{
		Method:        Linear,
		FillDirection: Backward,
		FillRegion:    nil,
		InPlace:       true,
	}

	fmt.Printf("\nInterpolate Series Linear Bkwd ...\n\n")
	fmt.Println("before:")
	fmt.Println(data.Values)

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	fmt.Println("after:")
	fmt.Println(data.Values)
}

func TestInterpolateSeriesLinearFillBoth(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 29.33, nil, nil, nil, 21.7, 35.14, nil, nil, 50.66, nil)

	opts := InterpolateOptions{
		Method:        Linear,
		FillDirection: Forward | Backward,
		FillRegion:    &[]InterpolationFillRegion{Interpolation}[0],
		InPlace:       true,
	}

	fmt.Printf("\nInterpolate Series Linear Both (Fwd and Bkwd) \n(Limit Area of Inner only) ...\n\n")
	fmt.Println("before:")
	fmt.Println(data.Values)

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	fmt.Println("after:")
	fmt.Println(data.Values)
}
