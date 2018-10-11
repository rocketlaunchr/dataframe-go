package dataframe

// Range is used to specify a range.
// Both Start and End are inclusive.
// A nil value means no limit.
type Range struct {
	Start *int
	End   *int
}

// RangeFinite returns a Range that has a
// finite span.
func RangeFinite(start, end int) Range {
	return Range{
		Start: &[]int{start}[0],
		End:   &[]int{end}[0],
	}
}
