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

func TestGuessTimeFreq(t *testing.T) {

	ctx := context.Background()
	timeFreq := "1Y1M3W"
	reverse := false
	now := time.Date(2020, 2, 13, 22, 25, 28, 0, time.UTC)

	opts := NewSeriesTimeOptions{
		Size: &[]int{10}[0],
	}

	ts, _ := NewSeriesTime(ctx, "Time Series", timeFreq, now, reverse, opts)

	guess, gReverse, err := GuessTimeFreq(ctx, ts, GuessTimeFreqOptions{})
	if err != nil {
		t.Errorf("error encountered: %v", err)
	} else {
		if guess != timeFreq || reverse != gReverse {
			guessPD, err1 := time.ParseDuration(guess)
			timeFreqPD, err2 := time.ParseDuration(timeFreq)
			if err1 != nil || err2 != nil || (guessPD != timeFreqPD || reverse != gReverse) {
				t.Errorf("expected: %v actual %v", timeFreq, guess)
			}
		}
	}
}

func TestGuessHintTimeFreq(t *testing.T) {

	ctx := context.Background()
	timeFreq := "3M2W"
	reverse := false
	now := time.Date(2020, 2, 13, 22, 25, 28, 0, time.UTC)

	opts := NewSeriesTimeOptions{
		Size: &[]int{7}[0],
	}

	ts, err := NewSeriesTime(ctx, "Time Series", timeFreq, now, reverse, opts)
	if err != nil {
		t.Errorf("error encountered: %v", err)
	}

	hintGuess := "3M2W"
	guess, _, err := GuessTimeFreq(ctx, ts, GuessTimeFreqOptions{Hint: hintGuess})
	if err != nil {
		t.Errorf("error encountered: %v", err)
	}

	if guess != hintGuess {
		t.Errorf("error: actual guess: %s not same as hint guess: %s\n", guess, hintGuess)
	}
}
