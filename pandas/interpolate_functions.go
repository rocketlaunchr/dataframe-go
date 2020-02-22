package pandas

import (
	"context"
	"errors"
	"fmt"
	"math"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// Given a start and end that are non-nil, this function forward fills.
func forwardFill(ctx context.Context, fs *dataframe.SeriesFloat64, start, end int, limit *int, ld InterpolationLimitDirection, la *InterpolationLimitArea) error {
	var (
		l, startOfSeg int
	)

	fmt.Println("before:")
	fmt.Println(fs.Values)

	startOfSeg = start
	for {

		var (
			left  *int
			right *int
			// fillVal float64
		)

		if limit != nil {
			l = *limit
		}

		fillVal := math.NaN() // reset fillVal for every iteration

		if startOfSeg >= end-1 {

			fillVal = fs.Values[end-1]
			if startOfSeg+1 == end && math.IsNaN(fs.Values[end]) {
				fs.Update(end, fillVal, dataframe.DontLock)

				if limit != nil {
					if limit != nil {
						l--
					} // decrease limit count
				}

			}
			break
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		// Step 1: Find segments that are inbetween non-nil values

		for i := startOfSeg; i <= end; i++ {
			currentVal := fs.Values[i]
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
		fillVal = fs.Values[*left]

		// Detect if there are nil values in between left and right segment
		if (*right - *left) > 1 { // possible nil values inbetween

			// Step 2: Fill in Inner region between left and right
			if ld.has(Forward) && ld.has(Backward) {
				// Loop through pos from middle of slice, outwards
				// https://play.golang.org/p/wjCoOSV4yyh

				for i := *left + 1; i < *right; i++ {

					if limit != nil && l <= 0 {
						break
					}

					var idx int

					if i%2 == 0 {
						idx = ((*left + 1) + (i / 2)) % len(fs.Values)
					} else {
						idx = ((*left + 1) + (len(fs.Values) - (1+i)/2)) % len(fs.Values)
					}
					val := fs.Values[idx]
					if math.IsNaN(val) { // verifying that the value is actually nan
						fs.Update(i, fillVal, dataframe.DontLock)

						if limit != nil {
							if limit != nil {
								l--
							} // decrease limit count
						}
					}

				}

			} else if ld.has(Backward) {
				// loop from end of slice backwards

				for i := *right - 1; i > *left; i-- {

					if limit != nil && l <= 0 {
						break
					}

					val := fs.Values[i]
					if math.IsNaN(val) { // verifying that the value is actually nan
						fs.Update(i, fillVal, dataframe.DontLock)

						if limit != nil {
							if limit != nil {
								l--
							} // decrease limit count
						}
					}
				}
			} else if ld.has(Forward) {
				// loop from start of slice forward

				for i := *left + 1; i < *right; i++ {

					if limit != nil && l <= 0 {
						break
					}

					val := fs.Values[i]
					if math.IsNaN(val) { // verifying that the value is actually nan
						fs.Update(i, fillVal, dataframe.DontLock)

						if limit != nil {
							if limit != nil {
								l--
							} // decrease limit count
						}
					}
				}
			} else {
				return errors.New("unknown interpolation limit direction(s) specified")
			}

		}

	}

	fmt.Println("after:")
	fmt.Println(fs.Values)
	return nil
}

// Given a start and end that are non-nil, this function backward fills.
func backwardFill(ctx context.Context, fs *dataframe.SeriesFloat64, start, end int, limit *int, ld InterpolationLimitDirection, la *InterpolationLimitArea) error {
	var (
		l, startOfSeg int
	)

	fmt.Println("before:")
	fmt.Println(fs.Values)

	startOfSeg = end
	for {

		var (
			left  *int
			right *int
			// fillVal float64
		)

		if limit != nil {
			l = *limit
		}

		fillVal := math.NaN() // reset fillVal for every iteration

		if startOfSeg <= start+1 {

			fillVal = fs.Values[start+1]
			if startOfSeg-1 == start && math.IsNaN(fs.Values[start]) {
				fs.Update(start, fillVal, dataframe.DontLock)
				if limit != nil {
					l--
				} // decrease limit count

			}
			break
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		// Step 1: Find segments that are inbetween non-nil values

		for i := startOfSeg; i >= start; i-- {
			currentVal := fs.Values[i]
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
		fillVal = fs.Values[*right]

		// Detect if there are nil values in between left and right segment
		if (*right - *left) > 1 { // possible nil values inbetween

			// Step 2: Fill in Inner region between left and right
			if ld.has(Forward) && ld.has(Backward) {
				// Loop through pos from middle of slice, outwards
				// https://play.golang.org/p/wjCoOSV4yyh

				for i := *left + 1; i < *right; i++ {

					if limit != nil && l <= 0 {
						break
					}

					var idx int

					if i%2 == 0 {
						idx = ((*left + 1) + (i / 2)) % len(fs.Values)
					} else {
						idx = ((*left + 1) + (len(fs.Values) - (1+i)/2)) % len(fs.Values)
					}
					val := fs.Values[idx]
					if math.IsNaN(val) { // verifying that the value is actually nan
						fs.Update(i, fillVal, dataframe.DontLock)
						if limit != nil {
							l--
						} // decrease limit count
					}
				}
			} else if ld.has(Backward) {
				// loop from end of slice backwards

				for i := *right - 1; i > *left; i-- {

					if limit != nil && l <= 0 {
						break
					}

					val := fs.Values[i]
					if math.IsNaN(val) { // verifying that the value is actually nan
						fs.Update(i, fillVal, dataframe.DontLock)
						if limit != nil {
							l--
						} // decrease limit count
					}
				}
			} else if ld.has(Forward) {
				// loop from start of slice forward

				for i := *left + 1; i < *right; i++ {

					if limit != nil && l <= 0 {
						break
					}

					val := fs.Values[i]
					if math.IsNaN(val) { // verifying that the value is actually nan
						fs.Update(i, fillVal, dataframe.DontLock)
						if limit != nil {
							l--
						} // decrease limit count
					}
				}
			} else {
				return errors.New("unknown interpolation limit direction(s) specified")
			}
		}
	}

	fmt.Println("after:")
	fmt.Println(fs.Values)
	return nil
}

// Given a start and end that are non-nil, this function applies linear interpolation.
func linearFill(ctx context.Context, fs *dataframe.SeriesFloat64, start, end int, limit *int, ld InterpolationLimitDirection, la *InterpolationLimitArea) error {
	var (
		l, startOfSeg int
	)

	fmt.Println("before:")
	fmt.Println(fs.Values)

	startOfSeg = start
	for {

		var (
			left  *int
			right *int
			inc   int
			// fillVal float64
		)

		if limit != nil {
			l = *limit
		}

		fillVal := math.NaN() // reset fillVal for every iteration

		if startOfSeg >= end-1 {
			break
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		// Step 1: Find segments that are inbetween non-nil values

		for i := startOfSeg; i <= end; i++ {
			currentVal := fs.Values[i]
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
					fillVal = (fs.Values[*left+inc] + fs.Values[*right]) / 2
					inc++

					if limit != nil && l <= 0 {
						break
					}

					var idx int

					if i%2 == 0 {
						idx = ((*left + 1) + (i / 2)) % len(fs.Values)
					} else {
						idx = ((*left + 1) + (len(fs.Values) - (1+i)/2)) % len(fs.Values)
					}

					val := fs.Values[idx]
					if math.IsNaN(val) { // verifying that the value is actually nan
						fs.Update(i, fillVal, dataframe.DontLock)
						if limit != nil {
							l--
						} // decrease limit count
					}

				}

			} else if ld.has(Backward) {
				// loop from end of slice backwards

				for i := *right - 1; i > *left; i-- {

					// For LinearFill
					// fill nan values with the mean of the values at previous and next index
					fillVal = (fs.Values[*left] + fs.Values[*right-inc]) / 2
					inc++

					if limit != nil && l <= 0 {
						break
					}

					val := fs.Values[i]
					if math.IsNaN(val) { // verifying that the value is actually nan
						fs.Update(i, fillVal, dataframe.DontLock)
						if limit != nil {
							l--
						} // decrease limit count
					}
				}
			} else if ld.has(Forward) {
				// loop from start of slice forward

				for i := *left + 1; i < *right; i++ {

					// For LinearFill
					// fill nan values with the mean of the values at previous and next index
					fillVal = (fs.Values[*left+inc] + fs.Values[*right]) / 2
					inc++

					if limit != nil && l <= 0 {
						break
					}

					val := fs.Values[i]
					if math.IsNaN(val) { // verifying that the value is actually nan
						fs.Update(i, fillVal, dataframe.DontLock)
						if limit != nil {
							l--
						} // decrease limit count
					}
				}
			} else {
				return errors.New("unknown interpolation limit direction(s) specified")
			}

		}

	}

	fmt.Println("after:")
	fmt.Println(fs.Values)
	return nil
}
