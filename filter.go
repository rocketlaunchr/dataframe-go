// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"context"
)

// FilterAction is the return value of FilterSeriesFn and FilterDataFrameFn.
type FilterAction int

const (
	// DROP is used to signify that a row must be dropped.
	DROP FilterAction = 0

	// KEEP is used to signify that a row must be kept.
	KEEP FilterAction = 1

	// CHOOSE is used to signify that a row must be kept.
	CHOOSE FilterAction = 1
)

type FilterOptions struct {

	// InPlace will perform the filter operation on the current Series or DataFrame.
	// If InPlace is not set, a new Series or DataFrame will be returned with rows based
	// on the filter operation. The original Series or DataFrame will be unmodified.
	InPlace bool

	Series []interface{} // Which series to keep

	DontLock bool
}

type FilterSeriesFn func(ctx context.Context, val interface{}, row, nRows int) (FilterAction, error)

// FilterFn is used by the Filter function to determine which rows are selected.
// If the function returns DROP, then the row is removed. If KEEP or CHOOSE is chosen, the row is kept.
type FilterFn func(ctx context.Context, vals map[interface{}]interface{}, row, nRows int) (FilterAction, error)

// FilterSeries is used to filter particular rows in a Series.
// If the InPlace option is set, this function returns nil. Instead s is modified "in place".
// Alternatively, a new Series is returned.
func FilterSeries(ctx context.Context, s Series, fn FilterSeriesFn, opts ...FilterOptions) (Series, error) {

	if fn == nil {
		panic("fn is required")
	}

	if len(opts) == 0 {
		opts = append(opts, FilterOptions{})
	}

	if !opts[0].InPlace {
		_, ok := s.(NewSerieser)
		if !ok {
			panic("s must implement NewSerieser interface if InPlace is false")
		}
	}

	if !opts[0].DontLock {
		s.Lock()
		defer s.Unlock()
	}

	transfer := []int{}

	iterator := s.ValuesIterator(ValuesOptions{InitialRow: 0, Step: 1, DontReadLock: !opts[0].DontLock})

	for {
		row, val, nRow := iterator()
		if row == nil {
			break
		}

		fa, err := fn(ctx, val, *row, nRow)
		if err != nil {
			return nil, err
		}

		if fa == DROP {
			if opts[0].InPlace {
				transfer = append(transfer, *row)
			}
		} else if fa == KEEP || fa == CHOOSE {
			if !opts[0].InPlace {
				transfer = append(transfer, *row)
			}
		} else {
			panic("unrecognized FilterAction returned by fn")
		}
	}

	if !opts[0].InPlace {
		// Create a New Series
		ns := (s.(NewSerieser)).NewSeries(s.Name(), &SeriesInit{Capacity: len(transfer)})
		for _, rowToTransfer := range transfer {
			val := s.Value(rowToTransfer, Options{DontLock: opts[0].DontLock})
			ns.Append(val, Options{DontLock: true})
		}
		return ns, nil
	} else {
		// Remove rows that need to be removed
		for idx := len(transfer) - 1; idx >= 0; idx-- {
			rowToRemove := transfer[idx]
			s.Remove(rowToRemove, Options{DontLock: opts[0].DontLock})
		}
	}

	return nil, nil
}

// Filter is used to filter particular rows in a DataFrame.
// If the InPlace option is set, this function returns nil. Instead df is modified "in place".
// Alternatively, a new DataFrame is returned.
func Filter(ctx context.Context, df *DataFrame, fn FilterFn, opts ...FilterOptions) (*DataFrame, error) {

	if fn == nil {
		panic("fn is required")
	}

	if len(opts) == 0 {
		opts = append(opts, FilterOptions{})
	}

	if !opts[0].InPlace {
		for _, s := range df.Series {
			_, ok := s.(NewSerieser)
			if !ok {
				panic("all Series in DataFrame must implement NewSerieser interface if InPlace is false")
			}
		}
	}

	if !opts[0].DontLock {
		df.Lock()
		defer df.Unlock()
	}

	transfer := []int{}

	iterator := df.ValuesIterator(ValuesOptions{InitialRow: 0, Step: 1, DontReadLock: !opts[0].DontLock})

	for {
		row, vals, nRow := iterator()
		if row == nil {
			break
		}

		fa, err := fn(ctx, vals, *row, nRow)
		if err != nil {
			return nil, err
		}

		if fa == DROP {
			if opts[0].InPlace {
				transfer = append(transfer, *row)
			}
		} else if fa == KEEP || fa == CHOOSE {
			if !opts[0].InPlace {
				transfer = append(transfer, *row)
			}
		} else {
			panic("unrecognized FilterAction returned by fn")
		}
	}

	if !opts[0].InPlace {
		// Create all series
		seriess := []Series{}
		for i := range df.Series {
			seriess = append(seriess, (df.Series[i].(NewSerieser)).NewSeries(df.Series[i].Name(), &SeriesInit{Capacity: len(transfer)}))
		}

		// Create a new dataframe
		ndf := NewDataFrame(seriess...)

		for _, rowToTransfer := range transfer {
			vals := df.Row(rowToTransfer, opts[0].DontLock, SeriesName)
			ndf.Append(vals)
		}
		return ndf, nil
	} else {
		// Remove rows that need to be removed
		for idx := len(transfer) - 1; idx >= 0; idx-- {
			rowToRemove := transfer[idx]
			df.Remove(rowToRemove)
		}
	}

	return nil, nil
}
