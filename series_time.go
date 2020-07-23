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
	"time"

	"github.com/olekukonko/tablewriter"
)

// SeriesTime is used for series containing time.Time data.
type SeriesTime struct {
	valFormatter ValueToStringFormatter

	// Layout is for internal use only at the moment. Do not use.
	//
	// See: https://golang.org/pkg/time/#Parse
	Layout string

	lock sync.RWMutex
	name string

	// Values is exported to better improve interoperability with various sub-packages.
	//
	// WARNING: Do not modify.
	Values   []*time.Time
	nilCount int
}

// NewSeriesTime creates a new series with the underlying type as time.Time.
func NewSeriesTime(name string, init *SeriesInit, vals ...interface{}) *SeriesTime {
	s := &SeriesTime{
		name:     name,
		Values:   []*time.Time{},
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

	s.Values = make([]*time.Time, size, capacity)
	s.valFormatter = DefaultValueFormatter

	for idx, v := range vals {

		// Special case
		if idx == 0 {
			if ts, ok := vals[0].([]time.Time); ok {
				for idx, v := range ts {
					val := s.valToPointer(v)
					if idx < size {
						s.Values[idx] = val
					} else {
						s.Values = append(s.Values, val)
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
			s.Values[idx] = val
		} else {
			s.Values = append(s.Values, val)
		}
	}

	var lVals int
	if len(vals) > 0 {
		if ts, ok := vals[0].([]time.Time); ok {
			lVals = len(ts)
		} else {
			lVals = len(vals)
		}
	}

	if lVals < size {
		s.nilCount = s.nilCount + size - lVals
	}

	return s
}

// NewSeries creates a new initialized SeriesTime.
func (s *SeriesTime) NewSeries(name string, init *SeriesInit) Series {
	return NewSeriesTime(name, init)
}

// Name returns the series name.
func (s *SeriesTime) Name(opts ...Options) string {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return s.name
}

// Rename renames the series.
func (s *SeriesTime) Rename(n string, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	s.name = n
}

// Type returns the type of data the series holds.
func (s *SeriesTime) Type() string {
	return "time"
}

// NRows returns how many rows the series contains.
func (s *SeriesTime) NRows(opts ...Options) int {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return len(s.Values)
}

// Value returns the value of a particular row.
// The return value could be nil or the concrete type
// the data type held by the series.
// Pointers are never returned.
func (s *SeriesTime) Value(row int, opts ...Options) interface{} {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	val := s.Values[row]
	if val == nil {
		return nil
	}
	return *val
}

// ValueString returns a string representation of a
// particular row. The string representation is defined
// by the function set in SetValueToStringFormatter.
// By default, a nil value is returned as "NaN".
func (s *SeriesTime) ValueString(row int, opts ...Options) string {
	return s.valFormatter(s.Value(row, opts...))
}

// Prepend is used to set a value to the beginning of the
// series. val can be a concrete data type or nil. Nil
// represents the absence of a value.
func (s *SeriesTime) Prepend(val interface{}, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	// See: https://stackoverflow.com/questions/41914386/what-is-the-mechanism-of-using-append-to-prepend-in-go

	if cap(s.Values) > len(s.Values) {
		// There is already extra capacity so copy current values by 1 spot
		s.Values = s.Values[:len(s.Values)+1]
		copy(s.Values[1:], s.Values)
		s.Values[0] = s.valToPointer(val)
		return
	}

	// No room, new slice needs to be allocated:
	s.insert(0, val)
}

// Append is used to set a value to the end of the series.
// val can be a concrete data type or nil. Nil represents
// the absence of a value.
func (s *SeriesTime) Append(val interface{}, opts ...Options) int {
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
func (s *SeriesTime) Insert(row int, val interface{}, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.insert(row, val)
}

func (s *SeriesTime) insert(row int, val interface{}) {

	switch V := val.(type) {
	case []time.Time:
		var vals []*time.Time
		for _, v := range V {
			v := v
			vals = append(vals, &v)
		}
		s.Values = append(s.Values[:row], append(vals, s.Values[row:]...)...)
		return
	case []*time.Time:
		for _, v := range V {
			if v == nil {
				s.nilCount++
			}
		}
		s.Values = append(s.Values[:row], append(V, s.Values[row:]...)...)
		return
	}

	s.Values = append(s.Values, nil)
	copy(s.Values[row+1:], s.Values[row:])

	v := s.valToPointer(val)
	if v == nil {
		s.nilCount++
	}

	s.Values[row] = s.valToPointer(v)
}

// Remove is used to delete the value of a particular row.
func (s *SeriesTime) Remove(row int, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	if s.Values[row] == nil {
		s.nilCount--
	}

	s.Values = append(s.Values[:row], s.Values[row+1:]...)
}

// Reset is used clear all data contained in the Series.
func (s *SeriesTime) Reset(opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values = []*time.Time{}
	s.nilCount = 0
}

// Update is used to update the value of a particular row.
// val can be a concrete data type or nil. Nil represents
// the absence of a value.
func (s *SeriesTime) Update(row int, val interface{}, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	newVal := s.valToPointer(val)

	if s.Values[row] == nil && newVal != nil {
		s.nilCount--
	} else if s.Values[row] != nil && newVal == nil {
		s.nilCount++
	}

	s.Values[row] = newVal
}

// ValuesIterator will return an iterator that can be used to iterate through all the values.
func (s *SeriesTime) ValuesIterator(opts ...ValuesOptions) func() (*int, interface{}, int) {

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
		// Should this be on the outside?
		if !dontReadLock {
			s.lock.RLock()
			defer s.lock.RUnlock()
		}

		var t int
		if step > 0 {
			t = (len(s.Values)-initial-1)/step + 1
		} else {
			t = -initial/step + 1
		}

		if row > len(s.Values)-1 || row < 0 {
			// Don't iterate further
			return nil, nil, t
		}

		val := s.Values[row]
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

func (s *SeriesTime) valToPointer(v interface{}) *time.Time {
	switch val := v.(type) {
	case nil:
		return nil
	case *time.Time:
		if val == nil {
			return nil
		}
		return &[]time.Time{*val}[0]
	case time.Time:
		return &val
	case *string:
		if val == nil {
			return nil
		}
		sec, err := strconv.ParseInt(*val, 10, 64)
		if err != nil {
			_ = v.(time.Time) // Intentionally panic
		}
		return &[]time.Time{time.Unix(sec, 0)}[0]
	case string:
		sec, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			_ = v.(time.Time) // Intentionally panic
		}
		return &[]time.Time{time.Unix(sec, 0)}[0]
	default:
		_ = v.(time.Time) // Intentionally panic
		return nil
	}
}

// SetValueToStringFormatter is used to set a function
// to convert the value of a particular row to a string
// representation.
func (s *SeriesTime) SetValueToStringFormatter(f ValueToStringFormatter) {
	if f == nil {
		s.valFormatter = DefaultValueFormatter
		return
	}
	s.valFormatter = f
}

// Swap is used to swap 2 values based on their row position.
func (s *SeriesTime) Swap(row1, row2 int, opts ...Options) {
	if row1 == row2 {
		return
	}

	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values[row1], s.Values[row2] = s.Values[row2], s.Values[row1]
}

// IsEqualFunc returns true if a is equal to b.
func (s *SeriesTime) IsEqualFunc(a, b interface{}) bool {

	if a == nil {
		if b == nil {
			return true
		}
		return false
	}

	if b == nil {
		return false
	}
	t1 := a.(time.Time)
	t2 := b.(time.Time)

	return t1.Equal(t2)
}

// IsLessThanFunc returns true if a is less than b.
func (s *SeriesTime) IsLessThanFunc(a, b interface{}) bool {

	if a == nil {
		if b == nil {
			return true
		}
		return true
	}

	if b == nil {
		return false
	}
	t1 := a.(time.Time)
	t2 := b.(time.Time)

	return t1.Before(t2)
}

// Sort will sort the series.
// It will return true if sorting was completed or false when the context is canceled.
func (s *SeriesTime) Sort(ctx context.Context, opts ...SortOptions) (completed bool) {

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

		if s.Values[i] == nil {
			if s.Values[j] == nil {
				// both are nil
				return true
			}
			return true
		}

		if s.Values[j] == nil {
			// i has value and j is nil
			return false
		}
		// Both are not nil
		ti := *s.Values[i]
		tj := *s.Values[j]

		return ti.Before(tj)
	}

	if opts[0].Stable {
		sort.SliceStable(s.Values, sortFunc)
	} else {
		sort.Slice(s.Values, sortFunc)
	}

	return true
}

// Lock will lock the Series allowing you to directly manipulate
// the underlying slice with confidence.
func (s *SeriesTime) Lock() {
	s.lock.Lock()
}

// Unlock will unlock the Series that was previously locked.
func (s *SeriesTime) Unlock() {
	s.lock.Unlock()
}

// Copy will create a new copy of the series.
// It is recommended that you lock the Series before attempting
// to Copy.
func (s *SeriesTime) Copy(r ...Range) Series {

	if len(s.Values) == 0 {
		return &SeriesTime{
			valFormatter: s.valFormatter,
			name:         s.name,
			Values:       []*time.Time{},
			nilCount:     s.nilCount,
		}
	}

	if len(r) == 0 {
		r = append(r, Range{})
	}

	start, end, err := r[0].Limits(len(s.Values))
	if err != nil {
		panic(err)
	}

	// Copy slice
	x := s.Values[start : end+1]
	newSlice := append(x[:0:0], x...)

	return &SeriesTime{
		valFormatter: s.valFormatter,
		name:         s.name,
		Values:       newSlice,
		nilCount:     s.nilCount,
	}
}

// Table will produce the Series in a table.
func (s *SeriesTime) Table(opts ...TableOptions) string {

	if len(opts) == 0 {
		opts = append(opts, TableOptions{R: &Range{}})
	}

	if !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	data := [][]string{}

	headers := []string{"", s.name} // row header is blank
	footers := []string{fmt.Sprintf("%dx%d", len(s.Values), 1), s.Type()}

	if len(s.Values) > 0 {

		start, end, err := opts[0].R.Limits(len(s.Values))
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
func (s *SeriesTime) String() string {

	count := len(s.Values)

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

	for row := range s.Values {
		out = out + s.ValueString(row, dontLock) + " "
	}
	return out + "]"
}

// ContainsNil will return whether or not the series contains any nil values.
func (s *SeriesTime) ContainsNil(opts ...Options) bool {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return s.nilCount > 0
}

// NilCount will return how many nil values are in the series.
func (s *SeriesTime) NilCount(opts ...NilCountOptions) (int, error) {
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

	start, end, err := r.Limits(len(s.Values))
	if err != nil {
		return 0, err
	}

	if start == 0 && end == len(s.Values)-1 {
		return s.nilCount, nil
	}

	var nilCount int

	for i := start; i <= end; i++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		if s.Values[i] == nil {

			if opts[0].StopAtOneNil {
				return 1, nil
			}

			nilCount++
		}
	}

	return nilCount, nil
}

// ToSeriesInt64 will convert the Series to a SeriesInt64. The time format is Unix seconds.
// The operation does not lock the Series.
func (s *SeriesTime) ToSeriesInt64(ctx context.Context, removeNil bool, conv ...func(interface{}) (*int64, error)) (*SeriesInt64, error) {

	ec := NewErrorCollection()

	ss := NewSeriesInt64(s.name, &SeriesInit{Capacity: s.NRows(dontLock)})

	for row, rowVal := range s.Values {

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
				cv := (*rowVal).Unix()
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

// ToSeriesFloat64 will convert the Series to a SeriesFloat64. The time format is Unix seconds.
// The operation does not lock the Series.
func (s *SeriesTime) ToSeriesFloat64(ctx context.Context, removeNil bool, conv ...func(interface{}) (float64, error)) (*SeriesFloat64, error) {

	ec := NewErrorCollection()

	ss := NewSeriesFloat64(s.name, &SeriesInit{Capacity: s.NRows(dontLock)})

	for row, rowVal := range s.Values {

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
				cv := float64((*rowVal).Unix())
				ss.Values = append(ss.Values, cv)
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
func (s *SeriesTime) ToSeriesMixed(ctx context.Context, removeNil bool, conv ...func(interface{}) (interface{}, error)) (*SeriesMixed, error) {
	ec := NewErrorCollection()

	ss := NewSeriesMixed(s.name, &SeriesInit{Capacity: s.NRows(dontLock)})

	for row, rowVal := range s.Values {

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
				cv := (*rowVal).Unix()
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
func (s *SeriesTime) FillRand(src rand.Source, probNil float64, rander Rander, opts ...FillRandOptions) {

	rng := rand.New(src)

	capacity := cap(s.Values)
	length := len(s.Values)
	s.nilCount = 0

	for i := 0; i < length; i++ {
		if rng.Float64() < probNil {
			// nil
			s.Values[i] = nil
			s.nilCount++
		} else {
			s.Values[i] = &[]time.Time{time.Unix(int64(rander.Rand()), 0)}[0]
		}
	}

	if capacity > length {
		excess := capacity - length
		for i := 0; i < excess; i++ {
			if rng.Float64() < probNil {
				// nil
				s.Values = append(s.Values, nil)
				s.nilCount++
			} else {
				s.Values = append(s.Values, &[]time.Time{time.Unix(int64(rander.Rand()), 0)}[0])
			}
		}
	}
}

// IsEqual returns true if s2's values are equal to s.
func (s *SeriesTime) IsEqual(ctx context.Context, s2 Series, opts ...IsEqualOptions) (bool, error) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	// Check type
	ts, ok := s2.(*SeriesTime)
	if !ok {
		return false, nil
	}

	// Check number of values
	if len(s.Values) != len(ts.Values) {
		return false, nil
	}

	// Check name
	if len(opts) != 0 && opts[0].CheckName {
		if s.name != ts.name {
			return false, nil
		}
	}

	// Check values
	for i, v := range s.Values {
		if err := ctx.Err(); err != nil {
			return false, err
		}

		if v == nil {
			if ts.Values[i] == nil {
				// Both are nil
				continue
			} else {
				return false, nil
			}
		}

		if !(*v).Equal(*ts.Values[i]) {
			return false, nil
		}
	}

	return true, nil
}
