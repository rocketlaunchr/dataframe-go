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
