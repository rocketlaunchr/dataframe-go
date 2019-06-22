package dataframe

import (
	"context"
	"runtime"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Search method is used to search for particular values in a given Series.
// It will return a slice containing ranges of row numbers
func Search(ctx context.Context, s Series, lower, upper interface{}, r ...Range) ([]Range, error) {
	// lock the series
	s.Lock()
	defer s.Unlock()

	// final output
	var rangeOutput []Range

	// set start and end from r
	if len(r) == 0 {
		r = append(r, Range{})
	}

	start, end, err := r[0].Limits(s.NRows())
	if err != nil {
		return nil, err
	}

	// get total num of rows to divide
	rangeCount := end - start

	// Divide search range amongst number of cores

	// Find how many cores are in the current cpu
	cpuNum := runtime.NumCPU()

	// Set Go to run parallel on multiple avaialable processor cores
	runtime.GOMAXPROCS(cpuNum)

	// divide Margin, sharing range across available processors
	divMargin := rangeCount / cpuNum

	var wg sync.WaitGroup
	rowStart := start             // point to a starting row
	for i := 0; i < cpuNum; i++ { // Run concurrent loop according to num of cpu cores
		// increment waitgroup counter
		wg.Add(1)

		rowStop := rowStart + divMargin

		// launch goroutine function here
		g, ctx := errgroup.WithContext(ctx)

		for row := rowStart; row <= rowStop; row++ {
			// val := s.Value(row)

			g.Go(func() error {
				defer wg.Done()

				// checking for error in context
				// this will be repositioned
				if err := ctx.Err(); err != nil {
					return err
				}
				// [CONCURRENT FUNCTION TO BE IMPLEMENTED]
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}

		// make the new starting continue from the stop of previous iteration
		// for the next loop that is to run concurrently
		rowStart = rowStop + 1
	}
	// Wait for all search goroutines to finish executing
	wg.Wait()

	return rangeOutput, nil
}
