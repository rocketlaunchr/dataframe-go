package utime

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestUtime(t *testing.T) {
	ctx := context.Background()

	size := 10
	opts := NewSeriesTimeOptions{
		Size: &size,
	}

	timeSeries, err := NewSeriesTime(ctx, "Time Series", "1D", time.Now(), false, opts)
	if err != nil {
		t.Errorf("error encountered: %s", err)
	}
	fmt.Println(timeSeries.Table())
}
