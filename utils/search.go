// Copyright 2018 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package utils

import (
	"context"
	"golang.org/x/sync/errgroup"
	"runtime"
	"sync"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// Search is used to find particular values in a given Series.
// It will find all values that are between lower and upper bounds (inclusive).
// It will return a slice containing the rows which contain values within the bounds.
// If Search is canceled, an incomplete list of the rows "found so far" is returned.
//
// Example:
//
//  s1 := dataframe.NewSeriesInt64("", nil, 11, 10, 9, 8, 7, 6, 5, 23, 25, 2, 1, 5, 4)
//
//  fmt.Println(dataframe.Search(ctx, s1, int64(4), int64(6)))
//  // Output: [5 6 11 12]
//
func Search(ctx context.Context, s dataframe.Series, lower, upper interface{}, r ...dataframe.Range) ([]int, error) {

	s.Lock()
	defer s.Unlock()

	if len(r) == 0 {
		r = append(r, dataframe.Range{})
	}

	fullRowCount := s.NRows(dataframe.Options{DontLock: true})
	if fullRowCount == 0 {
		return []int{}, nil
	}

	var equalCheck bool
	if cmp.Equal(lower, upper, cmpopts.IgnoreUnexported()) {
		equalCheck = true
	}

	start, end, err := r[0].Limits(fullRowCount)
	if err != nil {
		return nil, err
	}

	nCores := runtime.NumCPU()

	// Group search range equally amongst each core
	div := (end - start + 1) / nCores

	subRanges := []dataframe.Range{}

	for i := 0; i < nCores; i++ {
		var subStart int
		var subEnd int

		if i != nCores-1 {
			subStart = i * div
			subEnd = (i+1)*div - 1
		} else {
			// last core
			subStart = i * div
			subEnd = end
		}

		subRanges = append(subRanges, dataframe.Range{
			Start: &subStart,
			End:   &subEnd,
		})
	}

	// Concurrently search each subRange for values in range
	var g errgroup.Group

	var mapProtect sync.Mutex
	mapRows := map[int][]int{} // For each core store the rows we have found so far

	for i := 0; i < nCores; i++ {
		i := i
		g.Go(func() error {

			rowsFound := []int{} // Store all rows that we have found

			defer func() {
				mapProtect.Lock()
				mapRows[i] = rowsFound
				mapProtect.Unlock()
			}()

			for row := *subRanges[i].Start; row < *subRanges[i].End+1; row++ {

				// Cancel for loop if context is canceled
				if err := ctx.Err(); err != nil {
					return err
				}

				val := s.Value(row, dataframe.Options{DontLock: true})

				// Check if val is in range
				if equalCheck {
					if s.IsEqualFunc(val, lower) {
						rowsFound = append(rowsFound, row)
					}
				} else {
					if !s.IsLessThanFunc(val, lower) && (s.IsLessThanFunc(val, upper) || s.IsEqualFunc(val, upper)) {
						rowsFound = append(rowsFound, row)
					}
				}

			}

			return nil
		})
	}

	err = g.Wait()

	// Convert rows found to Range slice
	var rows []int
	var count int
	for i := 0; i < nCores; i++ {
		count = count + len(mapRows[i])
	}
	rows = make([]int, 0, count)

	// Store found rows into []int
	for i := 0; i < nCores; i++ {
		foundRows := mapRows[i]
		rows = append(rows, foundRows...)
	}

	return rows, err
}
