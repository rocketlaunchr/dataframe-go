// Copyright 2019-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package utils

// Float64Seq will return a sequence of float64 values starting at start.
func Float64Seq(start, end, step float64, max ...int) []float64 {
	if len(max) > 0 && max[0] == 0 {
		return []float64{}
	}

	out := []float64{start}

	if step == 0 {
		return out
	}

	for {
		newVal := out[len(out)-1] + step

		if step > 0 {
			if newVal > end {
				break
			}
		} else {
			if newVal < end {
				break
			}
		}

		if len(max) > 0 && len(out) >= max[0] {
			break
		}

		out = append(out, newVal)
	}

	return out
}

// IntSeq will return a sequence of int values starting at start.
func IntSeq(start, end, step int, max ...int) []int {
	if len(max) > 0 && max[0] == 0 {
		return []int{}
	}

	out := []int{start}

	if step == 0 {
		return out
	}

	for {
		newVal := out[len(out)-1] + step

		if step > 0 {
			if newVal > end {
				break
			}
		} else {
			if newVal < end {
				break
			}
		}

		if len(max) > 0 && len(out) >= max[0] {
			break
		}

		out = append(out, newVal)
	}

	return out
}

// Int64Seq will return a sequence of int64 values starting at start.
func Int64Seq(start, end, step int64, max ...int) []int64 {
	if len(max) > 0 && max[0] == 0 {
		return []int64{}
	}

	out := []int64{start}

	if step == 0 {
		return out
	}

	for {
		newVal := out[len(out)-1] + step

		if step > 0 {
			if newVal > end {
				break
			}
		} else {
			if newVal < end {
				break
			}
		}

		if len(max) > 0 && len(out) >= max[0] {
			break
		}

		out = append(out, newVal)
	}

	return out
}
