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

// FilterOptions modifies the behaviour of the Filter function.
type FilterOptions struct {

	// InPlace will perform the filter operation on the current Series or DataFrame.
	// If InPlace is not set, a new Series or DataFrame will be returned with rows based
	// on the filter operation. The original Series or DataFrame will be unmodified.
	InPlace bool

	// DontLock can be set to true if the Series should not be locked.
	DontLock bool
}

// FilterSeriesFn is used by the Filter function to determine which rows are selected.
// If the function returns DROP, then the row is removed. If KEEP or CHOOSE is chosen, the row is kept.
type FilterSeriesFn func(val interface{}, row, nRows int) (FilterAction, error)

// FilterDataFrameFn is used by the Filter function to determine which rows are selected.
// If the function returns DROP, then the row is removed. If KEEP or CHOOSE is chosen, the row is kept.
type FilterDataFrameFn func(vals map[interface{}]interface{}, row, nRows int) (FilterAction, error)

// Filter is used to filter particular rows in a Series or DataFrame.
// If the InPlace option is set, the function returns nil. Instead the Series or DataFrame is modified "in place".
// Alternatively, a new Series or DataFrame is returned.
func Filter(ctx context.Context, sdf interface{}, fn interface{}, opts ...FilterOptions) (interface{}, error) {

	switch typ := sdf.(type) {
	case Series:
		s, err := filterSeries(ctx, typ, fn.(FilterSeriesFn), opts...)
		if s == nil {
			return nil, err
		}
		return s, err
	case *DataFrame:
		df, err := filterDataFrame(ctx, typ, fn.(FilterDataFrameFn), opts...)
		if df == nil {
			return nil, err
		}
		return df, err
	default:
		panic("df must be a Series or DataFrame")
	}

	return nil, nil
}

func filterSeries(ctx context.Context, s Series, fn FilterSeriesFn, opts ...FilterOptions) (Series, error) {

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

	iterator := s.ValuesIterator(ValuesOptions{InitialRow: 0, Step: 1, DontReadLock: true})

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		row, val, nRows := iterator()
		if row == nil {
			break
		}

		fa, err := fn(val, *row, nRows)
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
		ns := (s.(NewSerieser)).NewSeries(s.Name(dontLock), &SeriesInit{Capacity: len(transfer)})
		for _, rowToTransfer := range transfer {
			val := s.Value(rowToTransfer, dontLock)
			ns.Append(val, dontLock)
		}
		return ns, nil
	}

	// Remove rows that need to be removed
	for idx := len(transfer) - 1; idx >= 0; idx-- {
		rowToRemove := transfer[idx]
		s.Remove(rowToRemove, dontLock)
	}

	return nil, nil
}

func filterDataFrame(ctx context.Context, df *DataFrame, fn FilterDataFrameFn, opts ...FilterOptions) (*DataFrame, error) {

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

	iterator := df.ValuesIterator(ValuesOptions{InitialRow: 0, Step: 1, DontReadLock: true})

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		row, vals, nRow := iterator()
		if row == nil {
			break
		}

		fa, err := fn(vals, *row, nRow)
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
			seriess = append(seriess, (df.Series[i].(NewSerieser)).NewSeries(df.Series[i].Name(dontLock), &SeriesInit{Capacity: len(transfer)}))
		}

		// Create a new dataframe
		ndf := NewDataFrame(seriess...)

		for _, rowToTransfer := range transfer {
			vals := df.Row(rowToTransfer, true, SeriesName)
			ndf.Append(&dontLock, vals)
		}
		return ndf, nil
	}

	// Remove rows that need to be removed
	for idx := len(transfer) - 1; idx >= 0; idx-- {
		rowToRemove := transfer[idx]
		df.Remove(rowToRemove, dontLock)
	}

	return nil, nil
}
