// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"context"
)

// Mean returns the mean. All non-nil values are ignored.
func (s *SeriesFloat64) Mean(ctx context.Context) (float64, error) {

	sum, err := s.Sum(ctx)
	if err != nil {
		return 0, err
	}

	count := len(s.Values) - s.nilCount
	if count == 0 {
		return sum, nil
	}

	return sum / float64(count), nil
}

// Sum returns the sum of all non-nil values. If all values are nil, a NaN is returned.
// If opposing infinites are found, a NaN is also returned
func (s *SeriesFloat64) Sum(ctx context.Context) (float64, error) {

	count := len(s.Values)

	var posinfs int
	var neginfs int

	if count > 0 && count == s.nilCount {
		// All values are nil
		return nan(), nil
	}

	var sum float64

	for _, v := range s.Values {

		if err := ctx.Err(); err != nil {
			return 0, err
		}

		if isNaN(v) {
			continue
		} else if isInf(v, 1) {
			posinfs++
			sum = sum + v

			if neginfs > 0 {
				return nan(), nil
			}
		} else if isInf(v, -1) {
			neginfs++
			sum = sum + v

			if posinfs > 0 {
				return nan(), nil
			}
		} else {
			sum = sum + v
		}
	}

	return float64(sum), nil
}

// Mean returns the mean. All non-nil values are ignored.
func (s *SeriesInt64) Mean(ctx context.Context) (float64, error) {

	sum, err := s.Sum(ctx)
	if err != nil {
		return 0, err
	}

	count := len(s.values) - s.nilCount
	if count == 0 {
		return sum, nil
	}

	return sum / float64(count), nil
}

// Sum returns the sum of all non-nil values. If all values are nil, a
// NaN is returned.
func (s *SeriesInt64) Sum(ctx context.Context) (float64, error) {

	count := len(s.values)

	if count > 0 && count == s.nilCount {
		// All values are nil
		return nan(), nil
	}

	var sum int64

	for _, v := range s.values {

		if err := ctx.Err(); err != nil {
			return 0, err
		}

		if v != nil {
			sum = sum + *v
		}

	}

	return float64(sum), nil
}
