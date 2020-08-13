// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"bytes"
	"context"
	"fmt"
	"golang.org/x/exp/rand"
	"sort"
	"strconv"
	"sync"

	"github.com/olekukonko/tablewriter"
)

// SeriesInt64 is used for series containing int64 data.
type SeriesInt64 struct {
	valFormatter ValueToStringFormatter

	lock     sync.RWMutex
	name     string
	values   []*int64
	nilCount int
}

// NewSeriesInt64 creates a new series with the underlying type as int64.
func NewSeriesInt64(name string, init *SeriesInit, vals ...interface{}) *SeriesInt64 {
	s := &SeriesInt64{
		name:     name,
		values:   []*int64{},
		nilCount: 0,
	}

	var (
		size     int
		capacity int
	)

	if init != nil {
		size = init.Size
		capacity = init.Capacity
		if size > capacity {
			capacity = size
		}
	}

	s.values = make([]*int64, size, capacity)
	s.valFormatter = DefaultValueFormatter

	for idx, v := range vals {

		// Special case
		if idx == 0 {
			if is, ok := vals[0].([]int64); ok {
				for idx, v := range is {
					val := s.valToPointer(v)
					if idx < size {
						s.values[idx] = val
					} else {
						s.values = append(s.values, val)
					}
				}
				break
			}
		}

		val := s.valToPointer(v)
		if val == nil {
			s.nilCount++
		}

		if idx < size {
			s.values[idx] = val
		} else {
			s.values = append(s.values, val)
		}
	}

	var lVals int
	if len(vals) > 0 {
		if is, ok := vals[0].([]int64); ok {
			lVals = len(is)
		} else {
			lVals = len(vals)
		}
	}

	if lVals < size {
		s.nilCount = s.nilCount + size - lVals
	}

	return s
}

// NewSeries creates a new initialized SeriesInt64.
func (s *SeriesInt64) NewSeries(name string, init *SeriesInit) Series {
	return NewSeriesInt64(name, init)
}

// Name returns the series name.
func (s *SeriesInt64) Name(opts ...Options) string {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return s.name
}

// Rename renames the series.
func (s *SeriesInt64) Rename(n string, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	s.name = n
}

// Type returns the type of data the series holds.
func (s *SeriesInt64) Type() string {
	return "int64"
}

// NRows returns how many rows the series contains.
func (s *SeriesInt64) NRows(opts ...Options) int {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return len(s.values)
}

// Value returns the value of a particular row.
// The return value could be nil or the concrete type
// the data type held by the series.
// Pointers are never returned.
func (s *SeriesInt64) Value(row int, opts ...Options) interface{} {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	val := s.values[row]
	if val == nil {
		return nil
	}
	return *val
}

// ValueString returns a string representation of a
// particular row. The string representation is defined
// by the function set in SetValueToStringFormatter.
// By default, a nil value is returned as "NaN".
func (s *SeriesInt64) ValueString(row int, opts ...Options) string {
	return s.valFormatter(s.Value(row, opts...))
}

// Prepend is used to set a value to the beginning of the
// series. val can be a concrete data type or nil. Nil
// represents the absence of a value.
func (s *SeriesInt64) Prepend(val interface{}, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	// See: https://stackoverflow.com/questions/41914386/what-is-the-mechanism-of-using-append-to-prepend-in-go

	if cap(s.values) > len(s.values) {
		// There is already extra capacity so copy current values by 1 spot
		s.values = s.values[:len(s.values)+1]
		copy(s.values[1:], s.values)
		s.values[0] = s.valToPointer(val)
		return
	}

	// No room, new slice needs to be allocated:
	s.insert(0, val)
}

// Append is used to set a value to the end of the series.
// val can be a concrete data type or nil. Nil represents
// the absence of a value.
func (s *SeriesInt64) Append(val interface{}, opts ...Options) int {
	var locked bool
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
		locked = true
	}

	row := s.NRows(Options{DontLock: locked})
	s.insert(row, val)
	return row
}

// Insert is used to set a value at an arbitrary row in
// the series. All existing values from that row onwards
// are shifted by 1. val can be a concrete data type or nil.
// Nil represents the absence of a value.
func (s *SeriesInt64) Insert(row int, val interface{}, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.insert(row, val)
}

func (s *SeriesInt64) insert(row int, val interface{}) {
	switch V := val.(type) {
	case []int64:
		var vals []*int64
		for _, v := range V {
			v := v
			vals = append(vals, &v)
		}
		s.values = append(s.values[:row], append(vals, s.values[row:]...)...)
		return
	case []*int64:
		for _, v := range V {
			if v == nil {
				s.nilCount++
			}
		}
		s.values = append(s.values[:row], append(V, s.values[row:]...)...)
		return
	}

	s.values = append(s.values, nil)
	copy(s.values[row+1:], s.values[row:])

	v := s.valToPointer(val)
	if v == nil {
		s.nilCount++
	}

	s.values[row] = s.valToPointer(v)
}

// Remove is used to delete the value of a particular row.
func (s *SeriesInt64) Remove(row int, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	if s.values[row] == nil {
		s.nilCount--
	}

	s.values = append(s.values[:row], s.values[row+1:]...)
}

// Reset is used clear all data contained in the Series.
func (s *SeriesInt64) Reset(opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.values = []*int64{}
	s.nilCount = 0
}

// Update is used to update the value of a particular row.
// val can be a concrete data type or nil. Nil represents
// the absence of a value.
func (s *SeriesInt64) Update(row int, val interface{}, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	newVal := s.valToPointer(val)

	if s.values[row] == nil && newVal != nil {
		s.nilCount--
	} else if s.values[row] != nil && newVal == nil {
		s.nilCount++
	}

	s.values[row] = newVal
}

// ValuesIterator will return an iterator that can be used to iterate through all the values.
func (s *SeriesInt64) ValuesIterator(opts ...ValuesOptions) func() (*int, interface{}, int) {

	var (
		row  int
		step int = 1
	)

	var dontReadLock bool

	if len(opts) > 0 {
		dontReadLock = opts[0].DontReadLock

		row = opts[0].InitialRow
		step = opts[0].Step
		if step == 0 {
			panic("Step can not be zero")
		}
	}

	initial := row

	return func() (*int, interface{}, int) {
		if !dontReadLock {
			s.lock.RLock()
			defer s.lock.RUnlock()
		}

		var t int
		if step > 0 {
			t = (len(s.values)-initial-1)/step + 1
		} else {
			t = -initial/step + 1
		}

		if row > len(s.values)-1 || row < 0 {
			// Don't iterate further
			return nil, nil, t
		}

		val := s.values[row]
		var out interface{}
		if val == nil {
			out = nil
		} else {
			out = *val
		}
		row = row + step
		return &[]int{row - step}[0], out, t
	}
}

func (s *SeriesInt64) valToPointer(v interface{}) *int64 {
	switch val := v.(type) {
	case nil:
		return nil
	case *bool:
		if val == nil {
			return nil
		}
		if *val == true {
			return &[]int64{1}[0]
		}
		return &[]int64{0}[0]
	case bool:
		if val == true {
			return &[]int64{1}[0]
		}
		return &[]int64{0}[0]
	case *int:
		if val == nil {
			return nil
		}
		return &[]int64{int64(*val)}[0]
	case int:
		return &[]int64{int64(val)}[0]
	case *int64:
		if val == nil {
			return nil
		}
		return &[]int64{*val}[0]
	case int64:
		return &val
	case *string:
		if val == nil {
			return nil
		}
		i, err := strconv.ParseInt(*val, 10, 64)
		if err != nil {
			_ = v.(int64) // Intentionally panic
		}
		return &i
	case string:
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			_ = v.(int64) // Intentionally panic
		}
		return &i
	default:
		i, err := strconv.ParseInt(fmt.Sprintf("%v", v), 10, 64)
		if err != nil {
			_ = v.(int64) // Intentionally panic
		}
		return &i
	}
}

// SetValueToStringFormatter is used to set a function
// to convert the value of a particular row to a string
// representation.
func (s *SeriesInt64) SetValueToStringFormatter(f ValueToStringFormatter) {
	if f == nil {
		s.valFormatter = DefaultValueFormatter
		return
	}
	s.valFormatter = f
}

// Swap is used to swap 2 values based on their row position.
func (s *SeriesInt64) Swap(row1, row2 int, opts ...Options) {
	if row1 == row2 {
		return
	}

	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.values[row1], s.values[row2] = s.values[row2], s.values[row1]
}

// IsEqualFunc returns true if a is equal to b.
func (s *SeriesInt64) IsEqualFunc(a, b interface{}) bool {

	if a == nil {
		if b == nil {
			return true
		}
		return false
	}

	if b == nil {
		return false
	}
	t1 := a.(int64)
	t2 := b.(int64)

	return t1 == t2
}

// IsLessThanFunc returns true if a is less than b.
func (s *SeriesInt64) IsLessThanFunc(a, b interface{}) bool {

	if a == nil {
		if b == nil {
			return true
		}
		return true
	}

	if b == nil {
		return false
	}
	t1 := a.(int64)
	t2 := b.(int64)

	return t1 < t2
}

// Sort will sort the series.
// It will return true if sorting was completed or false when the context is canceled.
func (s *SeriesInt64) Sort(ctx context.Context, opts ...SortOptions) (completed bool) {

	defer func() {
		if x := recover(); x != nil {
			completed = false
		}
	}()

	if len(opts) == 0 {
		opts = append(opts, SortOptions{})
	}

	if !opts[0].DontLock {
		s.Lock()
		defer s.Unlock()
	}

	sortFunc := func(i, j int) (ret bool) {
		if err := ctx.Err(); err != nil {
			panic(err)
		}

		defer func() {
			if opts[0].Desc {
				ret = !ret
			}
		}()

		if s.values[i] == nil {
			if s.values[j] == nil {
				// both are nil
				return true
			}
			return true
		}

		if s.values[j] == nil {
			// i has value and j is nil
			return false
		}
		// Both are not nil
		ti := *s.values[i]
		tj := *s.values[j]

		return ti < tj
	}

	if opts[0].Stable {
		sort.SliceStable(s.values, sortFunc)
	} else {
		sort.Slice(s.values, sortFunc)
	}

	return true
}

// Lock will lock the Series allowing you to directly manipulate
// the underlying slice with confidence.
func (s *SeriesInt64) Lock() {
	s.lock.Lock()
}

// Unlock will unlock the Series that was previously locked.
func (s *SeriesInt64) Unlock() {
	s.lock.Unlock()
}

// Copy will create a new copy of the series.
// It is recommended that you lock the Series before attempting
// to Copy.
func (s *SeriesInt64) Copy(r ...Range) Series {

	if len(s.values) == 0 {
		return &SeriesInt64{
			valFormatter: s.valFormatter,
			name:         s.name,
			values:       []*int64{},
			nilCount:     s.nilCount,
		}
	}

	if len(r) == 0 {
		r = append(r, Range{})
	}

	start, end, err := r[0].Limits(len(s.values))
	if err != nil {
		panic(err)
	}

	// Copy slice
	x := s.values[start : end+1]
	newSlice := append(x[:0:0], x...)

	return &SeriesInt64{
		valFormatter: s.valFormatter,
		name:         s.name,
		values:       newSlice,
		nilCount:     s.nilCount,
	}
}

// Table will produce the Series in a table.
func (s *SeriesInt64) Table(opts ...TableOptions) string {

	if len(opts) == 0 {
		opts = append(opts, TableOptions{R: &Range{}})
	}

	if !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	data := [][]string{}

	headers := []string{"", s.name} // row header is blank
	footers := []string{fmt.Sprintf("%dx%d", len(s.values), 1), s.Type()}

	if len(s.values) > 0 {

		start, end, err := opts[0].R.Limits(len(s.values))
		if err != nil {
			panic(err)
		}

		for row := start; row <= end; row++ {
			sVals := []string{fmt.Sprintf("%d:", row), s.ValueString(row, dontLock)}
			data = append(data, sVals)
		}

	}

	var buf bytes.Buffer

	table := tablewriter.NewWriter(&buf)
	table.SetHeader(headers)
	for _, v := range data {
		table.Append(v)
	}
	table.SetFooter(footers)
	table.SetAlignment(tablewriter.ALIGN_CENTER)

	table.Render()

	return buf.String()
}

// String implements the fmt.Stringer interface. It does not lock the Series.
func (s *SeriesInt64) String() string {

	count := len(s.values)

	out := "[ "

	if count > 6 {
		idx := []int{0, 1, 2, count - 3, count - 2, count - 1}
		for j, row := range idx {
			if j == 3 {
				out = out + "... "
			}
			out = out + s.ValueString(row, dontLock) + " "
		}
		return out + "]"
	}

	for row := range s.values {
		out = out + s.ValueString(row, dontLock) + " "
	}
	return out + "]"
}

// ContainsNil will return whether or not the series contains any nil values.
func (s *SeriesInt64) ContainsNil(opts ...Options) bool {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return s.nilCount > 0
}

// NilCount will return how many nil values are in the series.
func (s *SeriesInt64) NilCount(opts ...NilCountOptions) (int, error) {
	if len(opts) == 0 {
		s.lock.RLock()
		defer s.lock.RUnlock()
		return s.nilCount, nil
	}

	if !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	var (
		ctx context.Context
		r   *Range
	)

	if opts[0].Ctx == nil {
		ctx = context.Background()
	} else {
		ctx = opts[0].Ctx
	}

	if opts[0].R == nil {
		r = &Range{}
	} else {
		r = opts[0].R
	}

	start, end, err := r.Limits(len(s.values))
	if err != nil {
		return 0, err
	}

	if start == 0 && end == len(s.values)-1 {
		return s.nilCount, nil
	}

	var nilCount int

	for i := start; i <= end; i++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		if s.values[i] == nil {

			if opts[0].StopAtOneNil {
				return 1, nil
			}

			nilCount++
		}
	}

	return nilCount, nil
}

// ToSeriesString will convert the Series to a SeriesString.
// The operation does not lock the Series.
func (s *SeriesInt64) ToSeriesString(ctx context.Context, removeNil bool, conv ...func(interface{}) (*string, error)) (*SeriesString, error) {

	ec := NewErrorCollection()

	ss := NewSeriesString(s.name, &SeriesInit{Capacity: s.NRows(dontLock)})

	for row, rowVal := range s.values {

		// Cancel operation
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if rowVal == nil {
			if removeNil {
				continue
			}
			ss.values = append(ss.values, nil)
			ss.nilCount++
		} else {
			if len(conv) == 0 {
				cv := strconv.FormatInt(*rowVal, 10)
				ss.values = append(ss.values, &cv)
			} else {
				cv, err := conv[0](rowVal)
				if err != nil {
					// interpret as nil
					ss.values = append(ss.values, nil)
					ss.nilCount++
					ec.AddError(&RowError{Row: row, Err: err}, false)
				} else {
					if cv == nil {
						ss.values = append(ss.values, nil)
						ss.nilCount++
					} else {
						ss.values = append(ss.values, cv)
					}
				}
			}
		}
	}

	if !ec.IsNil(false) {
		return ss, ec
	}

	return ss, nil
}

// ToSeriesFloat64 will convert the Series to a SeriesFloat64.
// The operation does not lock the Series.
func (s *SeriesInt64) ToSeriesFloat64(ctx context.Context, removeNil bool, conv ...func(interface{}) (float64, error)) (*SeriesFloat64, error) {

	ec := NewErrorCollection()

	ss := NewSeriesFloat64(s.name, &SeriesInit{Capacity: s.NRows(dontLock)})

	for row, rowVal := range s.values {

		// Cancel operation
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if rowVal == nil {
			if removeNil {
				continue
			}
			ss.Values = append(ss.Values, nan())
			ss.nilCount++
		} else {
			if len(conv) == 0 {
				ss.Values = append(ss.Values, float64(*rowVal))
			} else {
				cv, err := conv[0](rowVal)
				if err != nil {
					// interpret as nil
					ss.Values = append(ss.Values, nan())
					ss.nilCount++
					ec.AddError(&RowError{Row: row, Err: err}, false)
				} else {
					if isNaN(cv) {
						ss.nilCount++
					}
					ss.Values = append(ss.Values, cv)
				}
			}
		}
	}

	if !ec.IsNil(false) {
		return ss, ec
	}

	return ss, nil
}

// ToSeriesMixed will convert the Series to a SeriesMIxed.
// The operation does not lock the Series.
func (s *SeriesInt64) ToSeriesMixed(ctx context.Context, removeNil bool, conv ...func(interface{}) (interface{}, error)) (*SeriesMixed, error) {
	ec := NewErrorCollection()

	ss := NewSeriesMixed(s.name, &SeriesInit{Capacity: s.NRows(dontLock)})

	for row, rowVal := range s.values {

		// Cancel operation
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if rowVal == nil {
			if removeNil {
				continue
			}
			ss.values = append(ss.values, nil)
			ss.nilCount++
		} else {
			if len(conv) == 0 {
				cv := *rowVal
				ss.values = append(ss.values, cv)
			} else {
				cv, err := conv[0](rowVal)
				if err != nil {
					// interpret as nil
					ss.values = append(ss.values, nil)
					ss.nilCount++
					ec.AddError(&RowError{Row: row, Err: err}, false)
				} else {
					if cv == nil {
						ss.nilCount++
					}
					ss.values = append(ss.values, cv)
				}
			}
		}
	}

	if !ec.IsNil(false) {
		return ss, ec
	}

	return ss, nil
}

// FillRand will fill a Series with random data. probNil is a value between between 0 and 1 which
// determines if a row is given a nil value.
func (s *SeriesInt64) FillRand(src rand.Source, probNil float64, rander Rander, opts ...FillRandOptions) {

	rng := rand.New(src)

	capacity := cap(s.values)
	length := len(s.values)
	s.nilCount = 0

	for i := 0; i < length; i++ {
		if rng.Float64() < probNil {
			// nil
			s.values[i] = nil
			s.nilCount++
		} else {
			s.values[i] = &[]int64{int64(rander.Rand())}[0]
		}
	}

	if capacity > length {
		excess := capacity - length
		for i := 0; i < excess; i++ {
			if rng.Float64() < probNil {
				// nil
				s.values = append(s.values, nil)
				s.nilCount++
			} else {
				s.values = append(s.values, &[]int64{int64(rander.Rand())}[0])
			}
		}
	}
}

// IsEqual returns true if s2's values are equal to s.
func (s *SeriesInt64) IsEqual(ctx context.Context, s2 Series, opts ...IsEqualOptions) (bool, error) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	// Check type
	is, ok := s2.(*SeriesInt64)
	if !ok {
		return false, nil
	}

	// Check number of values
	if len(s.values) != len(is.values) {
		return false, nil
	}

	// Check name
	if len(opts) != 0 && opts[0].CheckName {
		if s.name != is.name {
			return false, nil
		}
	}

	// Check values
	for i, v := range s.values {
		if err := ctx.Err(); err != nil {
			return false, err
		}

		if v == nil {
			if is.values[i] == nil {
				// Both are nil
				continue
			} else {
				return false, nil
			}
		}

		if *v != *is.values[i] {
			return false, nil
		}
	}

	return true, nil
}
