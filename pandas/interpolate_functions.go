package pandas

import (
	"context"
	"errors"
	"fmt"
	"math"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func fill(ctx context.Context, fillFn func(int) float64, fs *dataframe.SeriesFloat64, omap *dataframe.OrderedMapIntFloat64, start, end int, dir InterpolationLimitDirection, limit *int) error {

	if end-start <= 1 {
		return nil
	}

	var added int

	Len := end - start - 1

	if dir.has(Forward) && dir.has(Backward) {

		for j := 0; j < Len; j++ {

			if err := ctx.Err(); err != nil {
				return err
			}

			var idx int
			if j%2 == 0 {
				idx = j / 2
			} else {
				idx = Len - (1+j)/2
			}

			if omap != nil {
				omap.Set(start+1+idx, fillFn(j))
			} else {
				fs.Update(start+1+idx, fillFn(j), dataframe.DontLock)
			}
			added++

			if limit != nil && added >= *limit {
				return nil
			}

		}

	} else if dir.has(Forward) {

		for j := 0; j < Len; j++ {

			if err := ctx.Err(); err != nil {
				return err
			}

			if omap != nil {
				omap.Set(start+1+j, fillFn(j))
			} else {
				fs.Update(start+1+j, fillFn(j), dataframe.DontLock)
			}
			added++

			if limit != nil && added >= *limit {
				return nil
			}
		}

	} else if dir.has(Backward) {

		for j := Len - 1; j >= 0; j-- {

			if err := ctx.Err(); err != nil {
				return err
			}

			if omap != nil {
				omap.Set(start+1+j, fillFn(j))
			} else {
				fs.Update(start+1+j, fillFn(j), dataframe.DontLock)
			}
			added++

			if limit != nil && added >= *limit {
				return nil
			}
		}

	} else {
		return errors.New("unknown interpolation limit direction(s) specified")
	}

	return nil
}

// Given a start and end that are non-nil, this function forward fills.
func forwardFill(ctx context.Context, fs *dataframe.SeriesFloat64, start, end int, limit *int, ld InterpolationLimitDirection, la *InterpolationLimitArea) error {
	var (
		startOfSeg int
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

		fillVal := math.NaN() // reset fillVal for every iteration

		if startOfSeg >= end-1 {

			fillVal = fs.Values[end-1]
			if startOfSeg+1 == end && math.IsNaN(fs.Values[end]) {
				fs.Update(end, fillVal, dataframe.DontLock)
			}
			break
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		// Step 1: Find segments that are inbetween non-nil values

		left, right = findSubSegment(fs, startOfSeg, end, ForwardFill)
		startOfSeg = *right

		if err := fill(ctx, func(idx int) float64 { return fs.Values[*left] }, fs, nil, *left, *right, ld, ForwardFill, limit); err != nil {
			return err
		}
	}

	fmt.Println("after:")
	fmt.Println(fs.Values)
	return nil
}

// Given a start and end that are non-nil, this function backward fills.
func backwardFill(ctx context.Context, fs *dataframe.SeriesFloat64, start, end int, limit *int, ld InterpolationLimitDirection, la *InterpolationLimitArea) error {
	var (
		startOfSeg int
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

		fillVal := math.NaN() // reset fillVal for every iteration

		if startOfSeg <= start+1 {

			fillVal = fs.Values[start+1]
			if startOfSeg-1 == start && math.IsNaN(fs.Values[start]) {
				fs.Update(start, fillVal, dataframe.DontLock)
			}
			break
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		// Step 1: Find segments that are inbetween non-nil values

		left, right = findSubSegment(fs, startOfSeg, start, BackwardFill)
		startOfSeg = *left // new startOfSeg for next itern

		if err := fill(ctx, func(idx int) float64 { return fs.Values[*right] }, fs, nil, *left, *right, ld, BackwardFill, limit); err != nil {
			return err
		}
	}

	fmt.Println("after:")
	fmt.Println(fs.Values)
	return nil
}

// Given a start and end that are non-nil, this function applies linear interpolation.
func linearFill(ctx context.Context, fs *dataframe.SeriesFloat64, start, end int, limit *int, ld InterpolationLimitDirection, la *InterpolationLimitArea) error {
	var (
		startOfSeg int
	)

	fmt.Println("before:")
	fmt.Println(fs.Values)

	startOfSeg = start
	for {

		var (
			left  *int
			right *int
			// inc   int
		)

		if startOfSeg >= end-1 {
			break
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		// Step 1: Find segments that are inbetween non-nil values

		left, right = findSubSegment(fs, startOfSeg, end, Linear)
		startOfSeg = *right

		// inc := &[]int{0}[0]
		if err := fill(ctx, func(idx int) float64 { return (fs.Values[*left] + fs.Values[*right]) / 2 }, fs, nil, *left, *right, ld, Linear, limit); err != nil {
			return err
		}
	}

	fmt.Println("after:")
	fmt.Println(fs.Values)
	return nil
}

func findSubSegment(s *dataframe.SeriesFloat64, start, end int, mthd InterpolateMethod) (*int, *int) {
	var (
		left, right *int
	)

	if mthd == ForwardFill {
		// loop moving forward
		for i := start; i <= end; i++ {
			currentVal := s.Values[i]
			if !math.IsNaN(currentVal) {
				// non-nil found
				if left == nil {
					left = &[]int{i}[0]

				} else {
					right = &[]int{i}[0]

					break
				}
			}
		}
	} else if mthd == BackwardFill {
		// loop going backward
		for i := start; i >= end; i-- {
			currentVal := s.Values[i]
			if !math.IsNaN(currentVal) {
				// non-nil found
				if right == nil {
					right = &[]int{i}[0]

				} else {
					left = &[]int{i}[0]

					break
				}
			}
		}
	} else if mthd == Linear {
		for i := start; i <= end; i++ {
			currentVal := s.Values[i]
			if !math.IsNaN(currentVal) {
				// non-nil found
				if left == nil {
					left = &[]int{i}[0]

				} else {
					right = &[]int{i}[0]

					break
				}
			}
		}
	} else {
		panic("unknown interpolate method passed into findSubSegment function.")
	}

	return left, right
}
