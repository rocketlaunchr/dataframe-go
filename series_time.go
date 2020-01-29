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

	lock     sync.RWMutex
	name     string
	values   []*time.Time
	nilCount int
}

// NewSeriesTime creates a new series with the underlying type as time.Time
func NewSeriesTime(name string, init *SeriesInit, vals ...interface{}) *SeriesTime {
	s := &SeriesTime{
		name:     name,
		values:   []*time.Time{},
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

	s.values = make([]*time.Time, size, capacity)
	s.valFormatter = DefaultValueFormatter

	for idx, v := range vals {
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

	if len(vals) < size {
		s.nilCount = s.nilCount + size - len(vals)
	}

	return s
}

// Name returns the series name.
func (s *SeriesTime) Name() string {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.name
}

// Rename renames the series.
func (s *SeriesTime) Rename(n string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.name = n
}

// Type returns the type of data the series holds.
func (s *SeriesTime) Type() string {
	return "time"
}

// NRows returns how many rows the series contains.
func (s *SeriesTime) NRows(options ...Options) int {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return len(s.values)
}

// Value returns the value of a particular row.
// The return value could be nil or the concrete type
// the data type held by the series.
// Pointers are never returned.
func (s *SeriesTime) Value(row int, options ...Options) interface{} {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
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
func (s *SeriesTime) ValueString(row int, options ...Options) string {
	return s.valFormatter(s.Value(row, options...))
}

// Prepend is used to set a value to the beginning of the
// series. val can be a concrete data type or nil. Nil
// represents the absence of a value.
func (s *SeriesTime) Prepend(val interface{}, options ...Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
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
func (s *SeriesTime) Append(val interface{}, options ...Options) int {
	var locked bool
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
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
func (s *SeriesTime) Insert(row int, val interface{}, options ...Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
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
		s.values = append(s.values[:row], append(vals, s.values[row:]...)...)
		return
	case []*time.Time:
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
func (s *SeriesTime) Remove(row int, options ...Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	if s.values[row] == nil {
		s.nilCount--
	}

	s.values = append(s.values[:row], s.values[row+1:]...)
}

// Reset is used clear all data contained in the Series.
func (s *SeriesTime) Reset(options ...Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.values = []*time.Time{}
	s.nilCount = 0
}

// Update is used to update the value of a particular row.
// val can be a concrete data type or nil. Nil represents
// the absence of a value.
func (s *SeriesTime) Update(row int, val interface{}, options ...Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
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
func (s *SeriesTime) Swap(row1, row2 int, options ...Options) {
	if row1 == row2 {
		return
	}

	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.values[row1], s.values[row2] = s.values[row2], s.values[row1]
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

		return ti.Before(tj)
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

	if len(s.values) == 0 {
		return &SeriesTime{
			valFormatter: s.valFormatter,
			name:         s.name,
			values:       []*time.Time{},
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

	return &SeriesTime{
		valFormatter: s.valFormatter,
		name:         s.name,
		values:       newSlice,
		nilCount:     s.nilCount,
	}
}

// Table will produce the Series in a table.
func (s *SeriesTime) Table(r ...Range) string {

	s.lock.RLock()
	defer s.lock.RUnlock()

	if len(r) == 0 {
		r = append(r, Range{})
	}

	data := [][]string{}

	headers := []string{"", s.name} // row header is blank
	footers := []string{fmt.Sprintf("%dx%d", len(s.values), 1), s.Type()}

	if len(s.values) > 0 {

		start, end, err := r[0].Limits(len(s.values))
		if err != nil {
			panic(err)
		}

		for row := start; row <= end; row++ {
			sVals := []string{fmt.Sprintf("%d:", row), s.ValueString(row, Options{DontLock: true})}
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

// String implements Stringer interface.
func (s *SeriesTime) String() string {
	s.lock.RLock()
	defer s.lock.RUnlock()

	count := len(s.values)

	out := "[ "

	if count > 6 {
		idx := []int{0, 1, 2, count - 3, count - 2, count - 1}
		for j, row := range idx {
			if j == 3 {
				out = out + "... "
			}
			out = out + s.ValueString(row, Options{DontLock: true}) + " "
		}
		return out + "]"
	}

	for row := range s.values {
		out = out + s.ValueString(row, Options{DontLock: true}) + " "
	}
	return out + "]"
}

// ContainsNil will return whether or not the series contains any nil values.
func (s *SeriesTime) ContainsNil() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.nilCount > 0
}

// NilCount will return how many nil values are in the series.
func (s *SeriesTime) NilCount() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.nilCount
}

// ToSeriesInt64 will convert the Series to a SeriesInt64. The time format is Unix seconds.
// The operation does not lock the Series.
func (s *SeriesTime) ToSeriesInt64(ctx context.Context, removeNil bool, conv ...func(interface{}) (*int64, error)) (*SeriesInt64, error) {

	ec := NewErrorCollection()

	ss := NewSeriesInt64(s.name, &SeriesInit{Capacity: s.NRows(Options{DontLock: true})})

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

	ss := NewSeriesFloat64(s.name, &SeriesInit{Capacity: s.NRows(Options{DontLock: true})})

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

// FillRand will fill a Series with random data. probNil is a value between between 0 and 1 which
// determines if a row is given a nil value.
func (s *SeriesTime) FillRand(src rand.Source, probNil float64, rander Rander, opts ...FillRandOptions) {

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
			s.values[i] = &[]time.Time{time.Unix(int64(rander.Rand()), 0)}[0]
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
				s.values = append(s.values, &[]time.Time{time.Unix(int64(rander.Rand()), 0)}[0])
			}
		}
	}
}
