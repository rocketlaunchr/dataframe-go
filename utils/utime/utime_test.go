package utime

import (
	"context"
	"testing"
	"time"
)

func TestUtime(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	opts := NewSeriesTimeOptions{
		Size: &[]int{10}[0],
	}

	freqs := []string{
		"1D",
		"1M",
		"1h",
	}

	for _, f := range freqs {
		ts, err := NewSeriesTime(ctx, "Time Series", f, now, false, opts)
		if err != nil {
			t.Errorf("error encountered: %v", err)
		} else {
			err := ValidateSeriesTime(ctx, ts, f, ValidateSeriesTimeOptions{})
			if err != nil {
				t.Errorf("error encountered: %v", err)
			}
		}
	}

}
