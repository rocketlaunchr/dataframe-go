// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"context"
)

// ApplyDataFrameFn is used by Apply when used with DataFrames.
type ApplyDataFrameFn func(vals map[interface{}]interface{}, row, nRows int) map[interface{}]interface{}

// ApplySeriesFn is used by Apply when used with Series.
type ApplySeriesFn func(val interface{}, row, nRows int) interface{}

// Apply will run fn for each row in the Series or DataFrame and replace the existing value with the new
// value returned by fn. When sdf is a DataFrame, fn must be ApplyDataFrameFn. When sdf is a Series, fn must be ApplySeriesFn.
//
// As a special case, if fn returns nil when used with a DataFrame, the existing value is kept.
func Apply(ctx context.Context, sdf interface{}, fn interface{}, opts ...FilterOptions) (interface{}, error) {

	switch typ := sdf.(type) {
	case Series:
		s, err := applySeries(ctx, typ, fn.(ApplySeriesFn), opts...)
		if s == nil {
			return nil, err
		}
		return s, err
	case *DataFrame:
		df, err := applyDataFrame(ctx, typ, fn.(ApplyDataFrameFn), opts...)
		if df == nil {
			return nil, err
		}
		return df, err
	default:
		panic("sdf must be a Series or DataFrame")
	}

	return nil, nil
}

func applySeries(ctx context.Context, s Series, fn ApplySeriesFn, opts ...FilterOptions) (Series, error) {

	if fn == nil {
		panic("fn is required")
	}

	if len(opts) == 0 {
		opts = append(opts, FilterOptions{})
	}

	if !opts[0].DontLock {
		s.Lock()
		defer s.Unlock()
	}

	nRows := s.NRows(dontLock)

	var ns Series

	if !opts[0].InPlace {
		x, ok := s.(NewSerieser)
		if !ok {
			panic("s must implement NewSerieser interface if InPlace is false")
		}

		// Create a New Series
		ns = x.NewSeries(s.Name(dontLock), &SeriesInit{Capacity: nRows})
	}

	iterator := s.ValuesIterator(ValuesOptions{InitialRow: 0, Step: 1, DontReadLock: true})

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		row, val, nRows := iterator()
		if row == nil {
			break
		}

		newVal := fn(val, *row, nRows)

		if opts[0].InPlace {
			s.Update(*row, newVal, dontLock)
		} else {
			ns.Append(newVal, dontLock)
		}
	}

	if !opts[0].InPlace {
		return ns, nil
	}

	return nil, nil
}

func applyDataFrame(ctx context.Context, df *DataFrame, fn ApplyDataFrameFn, opts ...FilterOptions) (*DataFrame, error) {

	if fn == nil {
		panic("fn is required")
	}

	if len(opts) == 0 {
		opts = append(opts, FilterOptions{})
	}

	if !opts[0].DontLock {
		df.Lock()
		defer df.Unlock()
	}

	nRows := df.n

	var ndf *DataFrame

	if !opts[0].InPlace {

		// Create all series
		seriess := []Series{}
		for i := range df.Series {
			s := df.Series[i]

			x, ok := s.(NewSerieser)
			if !ok {
				panic("all Series in DataFrame must implement NewSerieser interface if InPlace is false")
			}

			seriess = append(seriess, x.NewSeries(df.Series[i].Name(dontLock), &SeriesInit{Capacity: nRows}))
		}

		// Create a new dataframe
		ndf = NewDataFrame(seriess...)
	}

	iterator := df.ValuesIterator(ValuesOptions{InitialRow: 0, Step: 1, DontReadLock: true})

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		row, vals, nRows := iterator()
		if row == nil {
			break
		}

		newVals := fn(vals, *row, nRows)

		if opts[0].InPlace {
			if newVals != nil {
				df.UpdateRow(*row, &dontLock, newVals)
			}
		} else {
			if newVals != nil {
				ndf.Append(&dontLock, newVals)
			} else {
				ndf.Append(&dontLock, vals)
			}
		}
	}

	if !opts[0].InPlace {
		return ndf, nil
	}

	return nil, nil
}
