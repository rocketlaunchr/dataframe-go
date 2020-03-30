package dataframe

import (
	"sort"
	"strings"
	"testing"
)

func TestRangeString(t *testing.T) {
	start := 1
	end := 10

	expected := []string{
		`Range:1-nil`,
		`Range:nil-10`,
		`Range:1-10`,
		`Range:nil-nil`,
	}
	rng1 := Range{Start: &start}
	rng2 := Range{End: &end}
	rng3 := Range{Start: &start, End: &end}
	rng4 := Range{}
	actual := []string{rng1.String(), rng2.String(), rng3.String(), rng4.String()}

	for i := range actual {

		if strings.TrimSpace(actual[i]) != strings.TrimSpace(expected[i]) {
			t.Errorf("wrong val: expected: %s actual: %s\n", expected[i], actual[i])
		}
	}
}

func TestRangeNRows(t *testing.T) {
	start := 1
	end := 10

	expected := 10
	rng := Range{Start: &start, End: &end}
	actual, err := rng.NRows()
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	if actual != expected {
		t.Errorf("wrong val: expected: %d actual: %d\n", expected, actual)
	}
}

func TestRangeLimits(t *testing.T) {
	start := 2
	end := 17

	negSt := -13
	negEnd := -7

	rng1 := Range{Start: &start, End: nil}
	rng2 := Range{Start: nil, End: &end}
	rng3 := Range{Start: &start, End: &end}
	rng4 := Range{Start: nil, End: nil}
	rng5 := Range{Start: &negSt, End: nil}
	rng6 := Range{Start: nil, End: &negEnd}
	rng7 := Range{Start: &negSt, End: &negEnd}

	expected := [][]int{
		[]int{2, 20},
		[]int{0, 17},
		[]int{2, 17},
		[]int{0, 20},
		[]int{8, 20},
		[]int{0, 14},
		[]int{8, 14},
	}

	actual := []Range{rng1, rng2, rng3, rng4, rng5, rng6, rng7}

	for i := range actual {
		st, nd, err := actual[i].Limits(21)
		if err != nil {
			t.Errorf("error encountered: %s\n", err)
		}
		if st != expected[i][0] || nd != expected[i][1] {
			t.Errorf("wrong val: expected => start: %d end: %d Got start: %d end: %d\n", start, end, st, nd)
		}
	}

}

func TestRangeFinite(t *testing.T) {
	start := 2
	end := 20

	actual := RangeFinite(start, end)
	expected := Range{Start: &start, End: &end}

	if strings.TrimSpace(actual.String()) != strings.TrimSpace(expected.String()) {
		t.Errorf("wrong val: expected: %s actual: %s\n", expected.String(), actual.String())
	}
}

func TestIntsToRanges(t *testing.T) {

	ints := []int{2, 4, 5, 6, 8, 10, 11, 45, 46, 47}
	sort.Ints(ints)

	rngs := IntsToRanges(ints)

	expected := []Range{
		Range{&[]int{2}[0], &[]int{2}[0]},
		Range{&[]int{4}[0], &[]int{6}[0]},
		Range{&[]int{8}[0], &[]int{8}[0]},
		Range{&[]int{10}[0], &[]int{11}[0]},
		Range{&[]int{45}[0], &[]int{47}[0]},
	}

	for i := range rngs {
		if strings.TrimSpace(rngs[i].String()) != strings.TrimSpace(expected[i].String()) {
			t.Errorf("wrong val: expected: %s actual: %s\n", expected[i].String(), rngs[i].String())
		}
	}

}
