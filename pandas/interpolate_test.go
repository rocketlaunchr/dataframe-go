package pandas

import (
	"context"
	"fmt"
	"testing"

	"github.com/rocketlaunchr/dataframe-go"
)

func TestInterpolateSeriesForwardFillFwd(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 50.3, nil, nil, 56.2, 45.34, nil, 39.26, nil)

	opts := InterpolateOptions{
		Method:         ForwardFill,
		LimitDirection: Forward,
		LimitArea:      Inner,
	}

	fmt.Printf("\nInterpolate Series ForwardFill Fwd...\n\n")

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
}

func TestInterpolateSeriesForwardFillBkwd(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 25.7, nil, nil, 36.6, 45.2, nil, 39.26, nil)

	opts := InterpolateOptions{
		Method:         ForwardFill,
		LimitDirection: Backward,
		LimitArea:      Inner,
	}

	fmt.Printf("\nInterpolate Series ForwardFill Bkwd...\n\n")
	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
}

func TestInterpolateSeriesForwardFillBoth(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 50.3, nil, nil, 56.2, 45.34, nil, 39.26, nil)

	opts := InterpolateOptions{
		Method:         ForwardFill,
		LimitDirection: (Forward | Backward),
		LimitArea:      Inner,
	}

	fmt.Printf("\nInterpolate Series ForwardFill Both (Fwd and Bkwd) ...\n\n")

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
}

func TestInterpolateSeriesBackwardFillBkwd(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 25.7, nil, nil, 36.6, 45.2, nil, 39.26, nil)

	opts := InterpolateOptions{
		Method:         BackwardFill,
		LimitDirection: Backward,
		LimitArea:      Inner,
	}

	fmt.Printf("\nInterpolate Series BackwardFill Bkwd...\n\n")
	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
}

func TestInterpolateSeriesBackwardFillFwd(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 50.3, nil, nil, 56.2, 45.34, nil, 39.26, nil)

	opts := InterpolateOptions{
		Method:         BackwardFill,
		LimitDirection: Forward,
		LimitArea:      Inner,
	}

	fmt.Printf("\nInterpolate Series BackwardFill Fwd...\n\n")

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
}

func TestInterpolateSeriesBackwardFillBoth(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 50.3, nil, nil, 56.2, 45.34, nil, 39.26, nil)

	opts := InterpolateOptions{
		Method:         BackwardFill,
		LimitDirection: (Forward | Backward),
		LimitArea:      Inner,
	}

	fmt.Printf("\nInterpolate Series BackwardFill Both (Fwd and Bkwd) ...\n\n")

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
}

func TestInterpolateSeriesLinearFwd(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 29.33, nil, nil, nil, 21.7, 35.14, nil, nil, 50.66, nil)

	opts := InterpolateOptions{
		Method:         Linear,
		LimitDirection: Forward,
		LimitArea:      Inner,
	}

	fmt.Printf("\nInterpolate Series Linear Fwd...\n\n")

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
}

func TestInterpolateSeriesLinearBkwd(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 29.33, nil, nil, nil, 21.7, 35.14, nil, nil, 50.66, nil)

	opts := InterpolateOptions{
		Method:         Linear,
		LimitDirection: Backward,
		LimitArea:      Inner,
	}

	fmt.Printf("\nInterpolate Series Linear Bkwd...\n\n")
	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
}

func TestInterpolateSeriesLinearBoth(t *testing.T) {
	ctx := context.Background()

	data := dataframe.NewSeriesFloat64("values", nil, nil, 29.33, nil, nil, nil, 21.7, 35.14, nil, nil, 50.66, nil)

	opts := InterpolateOptions{
		Method:         Linear,
		LimitDirection: (Forward | Backward),
		LimitArea:      Inner,
	}

	fmt.Printf("\nInterpolate Series Linear Both (Fwd and Bkwd) ...\n\n")

	_, err := Interpolate(ctx, data, opts)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
}
