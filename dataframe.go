// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"context"
	"errors"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
	"sync"
)

// DataFrame allows you to handle numerous
//series of data conveniently.
type DataFrame struct {
	lock   sync.RWMutex
	Series []Series
	n      int // Number of rows
}

// NewDataFrame creates a new dataframe.
func NewDataFrame(se ...Series) *DataFrame {

	df := &DataFrame{
		Series: []Series{},
	}

	if len(se) > 0 {
		var count *int
		names := map[string]struct{}{}

		for _, s := range se {
			if count == nil {
				count = &[]int{s.NRows()}[0]
				names[s.Name()] = struct{}{}
			} else {
				if *count != s.NRows() {
					panic("different number of rows in series")
				}
				if _, exists := names[s.Name()]; exists {
					panic("names of series must be unique")
				}
				names[s.Name()] = struct{}{}
			}
			df.Series = append(df.Series, s)
		}
		df.n = *count
	}

	return df
}

// NRows returns the number of rows of data.
// Each series must contain the same number of rows.
func (df *DataFrame) NRows(opts ...Options) int {
	if len(opts) == 0 || !opts[0].DontLock {
		df.lock.RLock()
		defer df.lock.RUnlock()
	}

	return df.n
}

// SeriesReturnOpt is used to control if Row/Values method returns
// the series index, series name or both as map keys.
type SeriesReturnOpt uint8

func (s SeriesReturnOpt) has(x SeriesReturnOpt) bool {
	return s&x != 0
}

const (
	// SeriesIdx forces the series' index to be returned as a map key.
	SeriesIdx SeriesReturnOpt = 1 << iota
	// SeriesName forces the series' name to be returned as a map key.
	SeriesName
)

// Row returns the series' values for a particular row.
//
// Example:
//
//  df.Row(5, false, dataframe.SeriesIdx|dataframe.SeriesName)
//
func (df *DataFrame) Row(row int, dontReadLock bool, retOpt ...SeriesReturnOpt) map[interface{}]interface{} {
	if !dontReadLock {
		df.lock.RLock()
		defer df.lock.RUnlock()
	}

	out := map[interface{}]interface{}{}

	for idx, aSeries := range df.Series {
		val := aSeries.Value(row)

		if len(retOpt) == 0 || retOpt[0].has(SeriesIdx) {
			out[idx] = val
		}
		if len(retOpt) == 0 || retOpt[0].has(SeriesName) {
			out[aSeries.Name()] = val
		}
	}

	return out
}

// ValuesOptions is used to modify the behavior of Values().
type ValuesOptions struct {

	// InitialRow represents the starting value for iterating.
	InitialRow int

	// Step represents by how much each iteration should step by.
	// It can be negative to represent iterating in reverse direction.
	// InitialRow should be adjusted to NRows()-1 if Step is negative.
	// If Step is 0, the function will panic.
	Step int

	// Don't apply read lock. This is useful if you intend to Write lock
	// the entire dataframe.
	DontReadLock bool
}

// ValuesIterator will return a function that can be used to iterate through all the values.
//
// The returned value is a map containing the name of the series (string) and the order of the series (int) as keys.
// You can reduce the keys in the map to only return the series name (SeriesName) or series index (SeriesIdx).
//
// Example:
//
//  iterator := df.ValuesIterator(dataframe.ValuesOptions{0, 1, true})
//
//  df.Lock()
//  for {
//     row, vals, _ := iterator(dataframe.SeriesName)
//     if row == nil {
//        break
//     }
//     fmt.Println(*row, vals)
//  }
//  df.Unlock()
//
func (df *DataFrame) ValuesIterator(options ...ValuesOptions) func(retOpt ...SeriesReturnOpt) (*int, map[interface{}]interface{}, int) {

	var (
		row  int
		step int = 1
	)

	var dontReadLock bool

	if len(options) > 0 {
		dontReadLock = options[0].DontReadLock

		row = options[0].InitialRow
		step = options[0].Step
		if step == 0 {
			panic("Step can not be zero")
		}
	}

	initial := row

	return func(retOpt ...SeriesReturnOpt) (*int, map[interface{}]interface{}, int) {
		if !dontReadLock {
			df.lock.RLock()
			defer df.lock.RUnlock()
		}

		var t int
		if step > 0 {
			t = (df.n-initial-1)/step + 1
		} else {
			t = -initial/step + 1
		}

		if row > df.n-1 || row < 0 {
			// Don't iterate further
			return nil, nil, t
		}

		out := map[interface{}]interface{}{}

		for idx, aSeries := range df.Series {
			val := aSeries.Value(row)

			if len(retOpt) == 0 || retOpt[0].has(SeriesIdx) {
				out[idx] = val
			}
			if len(retOpt) == 0 || retOpt[0].has(SeriesName) {
				out[aSeries.Name()] = val
			}
		}

		row = row + step

		return &[]int{row - step}[0], out, t
	}
}

// Prepend inserts a row at the beginning.
func (df *DataFrame) Prepend(opts *Options, vals ...interface{}) {
	df.Insert(0, opts, vals...)
}

// Append inserts a row at the end.
func (df *DataFrame) Append(opts *Options, vals ...interface{}) {
	df.Insert(df.n, opts, vals...)
}

// Insert adds a row to a particular position.
func (df *DataFrame) Insert(row int, opts *Options, vals ...interface{}) {
	if opts == nil || !opts.DontLock {
		df.lock.Lock()
		defer df.lock.Unlock()
	}

	df.insert(row, vals...)
}

func (df *DataFrame) insert(row int, vals ...interface{}) {

	if len(vals) > 0 {

		switch v := vals[0].(type) {
		case map[string]interface{}:

			// Check if number of vals is equal to number of series
			if len(v) != len(df.Series) {
				panic("no. of args not equal to no. of series")
			}

			for name, val := range v {
				col, err := df.NameToColumn(name, dontLock)
				if err != nil {
					panic(err)
				}
				df.Series[col].Insert(row, val)
			}
		case map[interface{}]interface{}:

			// Check if number of vals is equal to number of series
			names := map[string]struct{}{}

			for key := range v {
				switch kTyp := key.(type) {
				case int:
					names[df.Series[kTyp].Name(dontLock)] = struct{}{}
				case string:
					names[kTyp] = struct{}{}
				default:
					panic("unknown type in insert argument. Must be an int or string.")
				}
			}

			if len(names) != len(df.Series) {
				panic("no. of args not equal to no. of series")
			}

			for C, val := range v {
				switch CTyp := C.(type) {
				case int:
					df.Series[CTyp].Insert(row, val)
				case string:
					col, err := df.NameToColumn(CTyp, dontLock)
					if err != nil {
						panic(err)
					}
					df.Series[col].Insert(row, val)
				default:
					panic("unknown type in insert argument. Must be an int or string.")
				}
			}
		default:
			// Check if number of vals is equal to number of series
			if len(vals) != len(df.Series) {
				panic("no. of args not equal to no. of series")
			}

			for idx, val := range vals {
				df.Series[idx].Insert(row, val)
			}
		}

		df.n++
	}
}

// ClearRow makes an entire row nil.
func (df *DataFrame) ClearRow(row int, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		df.lock.Lock()
		defer df.lock.Unlock()
	}

	for i := range df.Series {
		df.Series[i].Update(row, nil, dontLock) //???
	}
}

// Remove deletes a row.
func (df *DataFrame) Remove(row int, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		df.lock.Lock()
		defer df.lock.Unlock()
	}

	for i := range df.Series {
		df.Series[i].Remove(row)
	}
	df.n--
}

// Update is used to update a specific entry.
// col can be the name of the series or the column number.
func (df *DataFrame) Update(row int, col interface{}, val interface{}, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		df.lock.Lock()
		defer df.lock.Unlock()
	}

	switch name := col.(type) {
	case string:
		_col, err := df.NameToColumn(name, dontLock)
		if err != nil {
			panic(err)
		}
		col = _col
	}

	df.Series[col.(int)].Update(row, val)
}

// UpdateRow will update an entire row.
func (df *DataFrame) UpdateRow(row int, opts *Options, vals ...interface{}) {
	if opts == nil || !opts.DontLock {
		df.lock.Lock()
		defer df.lock.Unlock()
	}

	if len(vals) > 0 {

		switch v := vals[0].(type) {
		case map[string]interface{}:
			for name, val := range v {
				col, err := df.NameToColumn(name, dontLock)
				if err != nil {
					panic(err)
				}
				df.Series[col].Update(row, val)
			}
		case map[interface{}]interface{}:
			for C, val := range v {
				switch CTyp := C.(type) {
				case int:
					df.Series[CTyp].Update(row, val)
				case string:
					col, err := df.NameToColumn(CTyp, dontLock)
					if err != nil {
						panic(err)
					}
					df.Series[col].Update(row, val)
				default:
					panic("unknown type in UpdateRow argument. Must be an int or string.")
				}
			}
		default:
			// Check if number of vals is equal to number of series
			if len(vals) != len(df.Series) {
				panic("no. of args not equal to no. of series")
			}

			for idx, val := range vals {
				df.Series[idx].Update(row, val)
			}
		}
	}
}

// Names will return a list of all the series names.
func (df *DataFrame) Names(opts ...Options) []string {
	if len(opts) == 0 || !opts[0].DontLock {
		df.lock.RLock()
		defer df.lock.RUnlock()
	}

	names := []string{}
	for _, aSeries := range df.Series {
		names = append(names, aSeries.Name())
	}

	return names
}

// MustNameToColumn returns the index of the series based on the name.
// The starting index is 0. If seriesName doesn't exist it panics.
func (df *DataFrame) MustNameToColumn(seriesName string, opts ...Options) int {
	col, err := df.NameToColumn(seriesName, opts...)
	if err != nil {
		panic(err)
	}

	return col
}

// NameToColumn returns the index of the series based on the name.
// The starting index is 0.
func (df *DataFrame) NameToColumn(seriesName string, opts ...Options) (int, error) {
	if len(opts) == 0 || !opts[0].DontLock {
		df.lock.RLock()
		defer df.lock.RUnlock()
	}

	for idx, aSeries := range df.Series {
		if aSeries.Name() == seriesName {
			return idx, nil
		}
	}

	return 0, errors.New("no series contains name")
}

// ReorderColumns reorders the columns based on an ordered list of
// column names. The length of newOrder must match the number of columns
// in the Dataframe. The column names in newOrder must be unique.
func (df *DataFrame) ReorderColumns(newOrder []string, opts ...Options) error {
	if len(opts) == 0 || !opts[0].DontLock {
		df.lock.Lock()
		defer df.lock.Unlock()
	}

	if len(newOrder) != len(df.Series) {
		return errors.New("length of newOrder must match number of columns")
	}

	// Check if newOrder contains duplicates
	fields := map[string]struct{}{}
	for _, v := range newOrder {
		fields[v] = struct{}{}
	}

	if len(fields) != len(df.Series) {
		return errors.New("newOrder must not contain duplicate values")
	}

	series := []Series{}

	for _, v := range newOrder {
		idx, err := df.NameToColumn(v, dontLock)
		if err != nil {
			return errors.New(err.Error() + ": " + v)
		}

		series = append(series, df.Series[idx])
	}

	df.Series = series

	return nil
}

// RemoveSeries will remove a Series from the Dataframe.
func (df *DataFrame) RemoveSeries(seriesName string, opts ...Options) error {
	if len(opts) == 0 || !opts[0].DontLock {
		df.lock.Lock()
		defer df.lock.Unlock()
	}

	idx, err := df.NameToColumn(seriesName, dontLock)
	if err != nil {
		return errors.New(err.Error() + ": " + seriesName)
	}

	df.Series = append(df.Series[:idx], df.Series[idx+1:]...)
	return nil
}

// AddSeries will add a Series to the end of the DataFrame, unless set by ColN.
func (df *DataFrame) AddSeries(s Series, colN *int, opts ...Options) error {
	if len(opts) == 0 || !opts[0].DontLock {
		df.lock.Lock()
		defer df.lock.Unlock()
	}

	if s.NRows(dontLock) != df.n {
		panic("different number of rows in series")
	}

	if colN == nil {
		df.Series = append(df.Series, s)
	} else {
		df.Series = append(df.Series, nil)
		copy(df.Series[*colN+1:], df.Series[*colN:])
		df.Series[*colN] = s
	}

	return nil
}

// Swap is used to swap 2 values based on their row position.
func (df *DataFrame) Swap(row1, row2 int, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		df.lock.Lock()
		defer df.lock.Unlock()
	}

	for idx := range df.Series {
		df.Series[idx].Swap(row1, row2)
	}
}

// Lock will lock the Dataframe allowing you to directly manipulate
// the underlying Series with confidence.
func (df *DataFrame) Lock(deepLock ...bool) {
	df.lock.Lock()

	if len(deepLock) > 0 && deepLock[0] {
		for i := range df.Series {
			df.Series[i].Lock()
		}
	}
}

// Unlock will unlock the Dataframe that was previously locked.
func (df *DataFrame) Unlock(deepUnlock ...bool) {
	if len(deepUnlock) > 0 && deepUnlock[0] {
		for i := range df.Series {
			df.Series[i].Unlock()
		}
	}

	df.lock.Unlock()
}

// Copy will create a new copy of the Dataframe.
// It is recommended that you lock the Dataframe
// before attempting to Copy.
func (df *DataFrame) Copy(r ...Range) *DataFrame {

	if len(r) == 0 {
		r = append(r, Range{})
	}

	seriess := []Series{}
	for i := range df.Series {
		seriess = append(seriess, df.Series[i].Copy(r...))
	}

	newDF := &DataFrame{
		Series: seriess,
	}

	if len(seriess) > 0 {
		newDF.n = seriess[0].NRows(dontLock)
	}

	return newDF
}

// FillRand will randomly fill all the Series in the Dataframe.
func (df *DataFrame) FillRand(src rand.Source, probNil float64, rander Rander, opts ...FillRandOptions) {
	for _, s := range df.Series {
		if sfr, ok := s.(FillRander); ok {
			sfr.FillRand(src, probNil, rander, opts...)
		}
	}
}

var errNotEqual = errors.New("not equal")

// IsEqual returns true if df2's values are equal to df.
func (df *DataFrame) IsEqual(ctx context.Context, df2 *DataFrame, opts ...IsEqualOptions) (bool, error) {
	if len(opts) == 0 || !opts[0].DontLock {
		df.lock.RLock()
		defer df.lock.RUnlock()
	}

	// Check if number of columns are the same
	if len(df.Series) != len(df2.Series) {
		return false, nil
	}

	// Check values
	g, newCtx := errgroup.WithContext(ctx)

	for i := range df.Series {
		i := i
		g.Go(func() error {

			eq, err := df.Series[i].IsEqual(newCtx, df2.Series[i], opts...)
			if err != nil {
				return err
			}

			if !eq {
				return errNotEqual
			}

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		if err == errNotEqual {
			return false, nil
		}
		return false, err
	}

	return true, nil
}
