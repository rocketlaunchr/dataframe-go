// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

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

// SearchOptions modifies the behavior of Search.
type SearchOptions struct {

	// Max is used to stop the search after it finds Max number of results.
	// When Max is set to nil (or 0), all results are sought.
	Max *int

	// NoConcurrency can be set to ensure that results are stable (& reproducible).
	// This can be useful if the context is canceled or a Max is set.
	NoConcurrency bool

	// R is used to limit the range of the Series for search purposes.
	R *dataframe.Range

	// DontLock can be set to true if the series should not be locked.
	DontLock bool
}

// Search is used to find particular values in a given Series.
// It will find all values that are between lower and upper bounds (inclusive).
// It will return a slice containing the rows which contain values within the bounds.
// If Search is canceled, an incomplete list of the rows "found so far" is returned.
// s will be locked for the duration of the operation.
//
// Example:
//
//  s1 := dataframe.NewSeriesInt64("", nil, 11, 10, 9, 8, 7, 6, 5, 23, 25, 2, 1, 5, 4)
//
//  fmt.Println(utils.Search(ctx, s1, int64(4), int64(6)))
//  // Output: [5 6 11 12]
//
func Search(ctx context.Context, s dataframe.Series, lower, upper interface{}, opts ...SearchOptions) ([]int, error) {

	if len(opts) == 0 {
		opts = append(opts, SearchOptions{R: &dataframe.Range{}})
	} else if opts[0].R == nil {
		opts[0].R = &dataframe.Range{}
	}

	if !opts[0].DontLock {
		s.Lock()
		defer s.Unlock()
	}

	fullRowCount := s.NRows(dataframe.DontLock)
	if fullRowCount == 0 {
		return []int{}, nil
	}

	var equalCheck bool
	if cmp.Equal(lower, upper, cmpopts.IgnoreUnexported()) {
		equalCheck = true
	}

	start, end, err := opts[0].R.Limits(fullRowCount)
	if err != nil {
		return nil, err
	}

	var nCores int
	if opts[0].NoConcurrency {
		nCores = 1
	} else {
		nCores = runtime.NumCPU()
	}

	// Group search range equally amongst each core
	div := (end - start + 1) / nCores

	subRanges := []dataframe.Range{}

	for i := 0; i < nCores; i++ {
		subStart := i * div
		var subEnd int

		if i != nCores-1 {
			subEnd = (i+1)*div - 1
		} else {
			// last core
			subEnd = end
		}

		subRanges = append(subRanges, dataframe.Range{
			Start: &subStart,
			End:   &subEnd,
		})
	}

	// Concurrently search each subRange for values in range
	g, newCtx := errgroup.WithContext(ctx)

	var mapProtect sync.Mutex
	mapRows := map[int][]int{} // For each core store the rows we have found so far
	var foundSoFarProtect sync.RWMutex
	foundSoFar := 0

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

				if nCores != 1 {
					// Exit early if max has been found already.
					if opts[0].Max != nil && *opts[0].Max != 0 {
						foundSoFarProtect.RLock()
						if foundSoFar >= *opts[0].Max {
							foundSoFarProtect.RUnlock()
							return nil
						}
						foundSoFarProtect.RUnlock()
					}
				}

				// Cancel for loop if context is canceled
				if err := newCtx.Err(); err != nil {
					return err
				}

				val := s.Value(row, dataframe.DontLock)

				// Check if val is in range
				if equalCheck {
					if s.IsEqualFunc(val, lower) {
						rowsFound = append(rowsFound, row)

						if nCores == 1 {
							foundSoFar++
							if opts[0].Max != nil && *opts[0].Max != 0 {
								if foundSoFar >= *opts[0].Max {
									return nil
								}
							}
						} else {
							if opts[0].Max != nil && *opts[0].Max != 0 {
								foundSoFarProtect.Lock()
								foundSoFar++
								if foundSoFar >= *opts[0].Max {
									foundSoFarProtect.Unlock()
									return nil
								}
								foundSoFarProtect.Unlock()
							}
						}
					}
				} else {
					if !s.IsLessThanFunc(val, lower) && (s.IsLessThanFunc(val, upper) || s.IsEqualFunc(val, upper)) {
						rowsFound = append(rowsFound, row)

						if nCores == 1 {
							foundSoFar++
							if opts[0].Max != nil && *opts[0].Max != 0 {
								if foundSoFar >= *opts[0].Max {
									return nil
								}
							}
						} else {
							if opts[0].Max != nil && *opts[0].Max != 0 {
								foundSoFarProtect.Lock()
								foundSoFar++
								if foundSoFar >= *opts[0].Max {
									foundSoFarProtect.Unlock()
									return nil
								}
								foundSoFarProtect.Unlock()
							}
						}
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

	// Ensure only max number of results is returned
	if opts[0].Max != nil && *opts[0].Max != 0 && len(rows) > *opts[0].Max {
		return rows[:*opts[0].Max], err
	}

	return rows, err
}
