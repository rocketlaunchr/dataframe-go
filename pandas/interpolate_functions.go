package pandas

import (
	"context"
	"errors"
	"fmt"
	"math"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func forwardFill(ctx context.Context, s *dataframe.SeriesFloat64, ld InterpolationLimitDirection, la InterpolationLimitArea, l int, r *dataframe.Range) (*dataframe.SeriesFloat64, error) {

	if r == nil {
		r = &dataframe.Range{}
	}

	start, end, err := r.Limits(len(s.Values))
	if err != nil {
		return nil, err
	}
	fmt.Println("before:")
	fmt.Println(s.Values)

	if la == Inner { // interpolate

		if ld == Forward {

			fillVal := math.NaN()
			for i := start; i <= end; i++ {

				if l <= 0 { // once limit gets to 0 return result
					return s, nil
				}
				val := s.Values[i]
				if math.IsNaN(val) {
					if (i - 1) < start { // skip if there are no previous value
						continue
					}
					fillVal = s.Values[i-1] // defintely value at index i-1 is nt nil
					if !math.IsNaN(fillVal) {
						s.Update(i, fillVal, dataframe.DontLock)
						l-- // decrease limit count
					}

				}
				fillVal = math.NaN() // reset fillVal for every iteration
			}

		} else if ld == Backward {

			fillVal := math.NaN()
			for i := end; i >= start; i-- {

				if l <= 0 {
					return s, nil
				}
				val := s.Values[i]
				if math.IsNaN(val) {

					// Get the forward fill value
					for j := i - 1; j >= start; j-- {
						if j < start {
							continue // skip if there are no previous value

						}
						if !math.IsNaN(s.Values[j]) {
							fillVal = s.Values[j]

							break
						}
					}
					if !math.IsNaN(fillVal) {
						s.Update(i, fillVal, dataframe.DontLock)
						l--
					}

				}
				fillVal = math.NaN() // reset fillVal for every iteration

			}
		} else {
			return nil, errors.New("unknown interpolation limit direction specified")
		}

	} else if la == Outer {
		// TODO: extrapolate
	} else {
		return nil, errors.New("unknown interpolation limit area specified")
	}

	fmt.Println("after:")
	fmt.Println(s.Values)
	return s, nil
}

func backwardFill(ctx context.Context, s *dataframe.SeriesFloat64, ld InterpolationLimitDirection, la InterpolationLimitArea, l int, r *dataframe.Range) (*dataframe.SeriesFloat64, error) {

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

		// Step 1: Find segments that are inbetween non-nil values

		var (
			left  *int
			right *int
		)

		for i := startOfSeg; i <= end; i++ {
			currentVal := s.Values[i]
			if !math.IsNaN(currentVal) {
				// non-nil found
				if left != nil {
					left = &[]int{i}[0]
				} else {
					right = &[]int{i}[0]
					startOfSeg = right
					break
				}
			}
		}

		// Step 2: Fill in Inner region between left and right
		if ld.has(Forward) {

		}

		if ld.has(Backward) {

		}

	}

	if la == Inner { // interpolate

		if ld == Backward {

			fillVal := math.NaN()
			for i := end; i >= start; i-- {

				if l <= 0 { // once limit gets to 0 return result
					return s, nil
				}
				val := s.Values[i]
				if math.IsNaN(val) {
					if (i + 1) > end { // skip if there are no previous value
						continue
					}
					fillVal = s.Values[i+1] // defintely value at index i+1 is nt nil
					if !math.IsNaN(fillVal) {
						s.Update(i, fillVal, dataframe.DontLock)
						l-- // decrease limit count
					}

				}
				fillVal = math.NaN() // reset fillVal for every iteration
			}

		} else if ld == Forward {

			fillVal := math.NaN()
			for i := start; i <= end; i++ {

				if l <= 0 {
					return s, nil
				}
				val := s.Values[i]
				if math.IsNaN(val) {

					// Get the backward fill value
					for j := i + 1; j <= end; j++ {
						if j > end {
							continue // skip if there are no previous value

						}
						if !math.IsNaN(s.Values[j]) {
							fillVal = s.Values[j]

							break
						}
					}
					if !math.IsNaN(fillVal) {
						s.Update(i, fillVal, dataframe.DontLock)
						l--
					}
				}
				fillVal = math.NaN() // reset fillVal for every iteration

			}
		} else {
			return nil, errors.New("unknown interpolation limit direction specified")
		}

	} else if la == Outer {
		// TODO: extrapolate
	} else {
		return nil, errors.New("unknown interpolation limit area specified")
	}

	fmt.Println("after:")
	fmt.Println(s.Values)
	return s, nil
}
