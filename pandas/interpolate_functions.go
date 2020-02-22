package pandas

import (
	"context"
	"errors"
	"fmt"
	"math"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func forwardFill(ctx context.Context, s *dataframe.SeriesFloat64, ld InterpolationLimitDirection, la InterpolationLimitArea, lim int, r *dataframe.Range) (*dataframe.SeriesFloat64, error) {

	var (
		l, startOfSeg int
	)

	if r == nil {
		r = &dataframe.Range{}
	}

	start, end, err := r.Limits(len(s.Values))
	if err != nil {
		return nil, err
	}
	fmt.Println("before:")
	fmt.Println(s.Values)

	startOfSeg = start
	for {

		var (
			left  *int
			right *int
			// fillVal float64
		)

		l = lim
		fillVal := math.NaN() // reset fillVal for every iteration

		if startOfSeg >= end-1 {

			fillVal = s.Values[end-1]
			if startOfSeg+1 == end && math.IsNaN(s.Values[end]) {
				s.Update(end, fillVal, dataframe.DontLock)
				l-- // decrease limit count

			}
			break
		}

		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Step 1: Find segments that are inbetween non-nil values

		for i := startOfSeg; i <= end; i++ {
			currentVal := s.Values[i]
			if !math.IsNaN(currentVal) {
				// non-nil found
				if left == nil {
					left = &[]int{i}[0]

				} else {
					right = &[]int{i}[0]
					startOfSeg = *right

					break
				}
			}
		} // For ForwardFill
		// fill nan values with the value at the left
		fillVal = s.Values[*left]

		// Detect if there are nil values in between left and right segment
		if (*right - *left) > 1 { // possible nil values inbetween

			// Step 2: Fill in Inner region between left and right
			if ld.has(Forward) && ld.has(Backward) {
				// Loop through pos from middle of slice, outwards
				// https://play.golang.org/p/wjCoOSV4yyh

				for i := *left + 1; i < *right; i++ {

					if l <= 0 { // once limit gets to 0 break from fill loop
						break
					}

					var idx int

					if i%2 == 0 {
						idx = ((*left + 1) + (i / 2)) % len(s.Values)
					} else {
						idx = ((*left + 1) + (len(s.Values) - (1+i)/2)) % len(s.Values)
					}
					val := s.Values[idx]
					if math.IsNaN(val) { // verifying that the value is actually nan
						s.Update(i, fillVal, dataframe.DontLock)
						l-- // decrease limit count
					}

				}

			} else if ld.has(Backward) {
				// loop from end of slice backwards

				for i := *right - 1; i > *left; i-- {

					if l <= 0 { // once limit gets to 0 break from fill loop
						break
					}

					val := s.Values[i]
					if math.IsNaN(val) { // verifying that the value is actually nan
						s.Update(i, fillVal, dataframe.DontLock)
						l-- // decrease limit count
					}
				}
			} else if ld.has(Forward) {
				// loop from start of slice forward

				for i := *left + 1; i < *right; i++ {

					if l <= 0 { // once limit gets to 0 break from fill loop
						break
					}

					val := s.Values[i]
					if math.IsNaN(val) { // verifying that the value is actually nan
						s.Update(i, fillVal, dataframe.DontLock)
						l-- // decrease limit count
					}
				}
			} else {
				return nil, errors.New("unknown interpolation limit direction(s) specified")
			}

		}

	}

	fmt.Println("after:")
	fmt.Println(s.Values)
	return s, nil
}

func backwardFill(ctx context.Context, s *dataframe.SeriesFloat64, ld InterpolationLimitDirection, la InterpolationLimitArea, lim int, r *dataframe.Range) (*dataframe.SeriesFloat64, error) {
	var (
		l, startOfSeg int
	)

	if r == nil {
		r = &dataframe.Range{}
	}

	start, end, err := r.Limits(len(s.Values))
	if err != nil {
		return nil, err
	}
	fmt.Println("before:")
	fmt.Println(s.Values)

	startOfSeg = end
	for {

		var (
			left  *int
			right *int
			// fillVal float64
		)

		l = lim
		fillVal := math.NaN() // reset fillVal for every iteration

		if startOfSeg <= start+1 {

			fillVal = s.Values[start+1]
			if startOfSeg-1 == start && math.IsNaN(s.Values[start]) {
				s.Update(start, fillVal, dataframe.DontLock)
				l-- // decrease limit count

			}
			break
		}

		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Step 1: Find segments that are inbetween non-nil values

		for i := startOfSeg; i >= start; i-- {
			currentVal := s.Values[i]
			if !math.IsNaN(currentVal) {
				// non-nil found
				if right == nil {
					right = &[]int{i}[0]

				} else {
					left = &[]int{i}[0]
					startOfSeg = *left

					break
				}
			}
		} // For BackwardFill
		// fill nan values with the value at the right
		fillVal = s.Values[*right]

		// Detect if there are nil values in between left and right segment
		if (*right - *left) > 1 { // possible nil values inbetween

			// Step 2: Fill in Inner region between left and right
			if ld.has(Forward) && ld.has(Backward) {
				// Loop through pos from middle of slice, outwards
				// https://play.golang.org/p/wjCoOSV4yyh

				for i := *left + 1; i < *right; i++ {

					if l <= 0 { // once limit gets to 0 break from fill loop
						break
					}

					var idx int

					if i%2 == 0 {
						idx = ((*left + 1) + (i / 2)) % len(s.Values)
					} else {
						idx = ((*left + 1) + (len(s.Values) - (1+i)/2)) % len(s.Values)
					}
					val := s.Values[idx]
					if math.IsNaN(val) { // verifying that the value is actually nan
						s.Update(i, fillVal, dataframe.DontLock)
						l-- // decrease limit count
					}
				}
			} else if ld.has(Backward) {
				// loop from end of slice backwards

				for i := *right - 1; i > *left; i-- {

					if l <= 0 { // once limit gets to 0 break from fill loop
						break
					}

					val := s.Values[i]
					if math.IsNaN(val) { // verifying that the value is actually nan
						s.Update(i, fillVal, dataframe.DontLock)
						l-- // decrease limit count
					}
				}
			} else if ld.has(Forward) {
				// loop from start of slice forward

				for i := *left + 1; i < *right; i++ {

					if l <= 0 { // once limit gets to 0 break from fill loop
						break
					}

					val := s.Values[i]
					if math.IsNaN(val) { // verifying that the value is actually nan
						s.Update(i, fillVal, dataframe.DontLock)
						l-- // decrease limit count
					}
				}
			} else {
				return nil, errors.New("unknown interpolation limit direction(s) specified")
			}
		}
	}

	fmt.Println("after:")
	fmt.Println(s.Values)
	return s, nil
}

func linear(ctx context.Context, s *dataframe.SeriesFloat64, ld InterpolationLimitDirection, la InterpolationLimitArea, lim int, r *dataframe.Range) (*dataframe.SeriesFloat64, error) {

	var (
		l, startOfSeg int
	)

	if r == nil {
		r = &dataframe.Range{}
	}

	start, end, err := r.Limits(len(s.Values))
	if err != nil {
		return nil, err
	}
	fmt.Println("before:")
	fmt.Println(s.Values)

	startOfSeg = start
	for {

		var (
			left  *int
			right *int
			inc   int
			// fillVal float64
		)

		l = lim
		fillVal := math.NaN() // reset fillVal for every iteration

		if startOfSeg >= end-1 {
			break
		}

		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Step 1: Find segments that are inbetween non-nil values

		for i := startOfSeg; i <= end; i++ {
			currentVal := s.Values[i]
			if !math.IsNaN(currentVal) {
				// non-nil found
				if left == nil {
					left = &[]int{i}[0]

				} else {
					right = &[]int{i}[0]
					startOfSeg = *right

					break
				}
			}
		}

		// Detect if there are nil values in between left and right segment
		if (*right - *left) > 1 { // possible nil values inbetween

			// Step 2: Fill in Inner region between left and right
			if ld.has(Forward) && ld.has(Backward) {
				// Loop through pos from middle of slice, outwards
				// https://play.golang.org/p/wjCoOSV4yyh

				for i := *left + 1; i < *right; i++ {

					// For LinearFill
					// fill nan values with the mean of the values at previous and next index
					fillVal = (s.Values[*left+inc] + s.Values[*right]) / 2
					inc++

					if l <= 0 { // once limit gets to 0 break from fill loop
						break
					}

					var idx int

					if i%2 == 0 {
						idx = ((*left + 1) + (i / 2)) % len(s.Values)
					} else {
						idx = ((*left + 1) + (len(s.Values) - (1+i)/2)) % len(s.Values)
					}

					val := s.Values[idx]
					if math.IsNaN(val) { // verifying that the value is actually nan
						s.Update(i, fillVal, dataframe.DontLock)
						l-- // decrease limit count
					}

				}

			} else if ld.has(Backward) {
				// loop from end of slice backwards

				for i := *right - 1; i > *left; i-- {

					// For LinearFill
					// fill nan values with the mean of the values at previous and next index
					fillVal = (s.Values[*left] + s.Values[*right-inc]) / 2
					inc++

					if l <= 0 { // once limit gets to 0 break from fill loop
						break
					}

					val := s.Values[i]
					if math.IsNaN(val) { // verifying that the value is actually nan
						s.Update(i, fillVal, dataframe.DontLock)
						l-- // decrease limit count
					}
				}
			} else if ld.has(Forward) {
				// loop from start of slice forward

				for i := *left + 1; i < *right; i++ {

					// For LinearFill
					// fill nan values with the mean of the values at previous and next index
					fillVal = (s.Values[*left+inc] + s.Values[*right]) / 2
					inc++

					if l <= 0 { // once limit gets to 0 break from fill loop
						break
					}

					val := s.Values[i]
					if math.IsNaN(val) { // verifying that the value is actually nan
						s.Update(i, fillVal, dataframe.DontLock)
						l-- // decrease limit count
					}
				}
			} else {
				return nil, errors.New("unknown interpolation limit direction(s) specified")
			}

		}

	}

	fmt.Println("after:")
	fmt.Println(s.Values)
	return s, nil
}
