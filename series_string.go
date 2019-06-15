// Copyright 2018 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/olekukonko/tablewriter"
)

// SeriesString is used for series containing string data.
type SeriesString struct {
	valFormatter ValueToStringFormatter

	lock     sync.RWMutex
	name     string
	values   []*string
	nilCount uint
}

// NewSeriesString creates a new series with the underlying type as string
func NewSeriesString(name string, init *SeriesInit, vals ...interface{}) *SeriesString {
	s := &SeriesString{
		name:     name,
		values:   []*string{},
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

	s.nilCount = uint(size)
	s.values = make([]*string, size, capacity)
	s.valFormatter = DefaultValueFormatter

	for idx, v := range vals {
		if idx < size {
			if s.values[idx] == nil && s.valToPointer(v) != nil {
				s.nilCount--
			}
			if s.values[idx] != nil && s.valToPointer(v) == nil {
				s.nilCount++
			}
			s.values[idx] = s.valToPointer(v)
		} else {
			if s.valToPointer(v) == nil {
				s.nilCount++
			}
			s.values = append(s.values, s.valToPointer(v))
		}
	}

	return s
}

// Name returns the series name.
func (s *SeriesString) Name() string {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.name
}

// Rename renames the series.
func (s *SeriesString) Rename(n string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.name = n
}

// Type returns the type of data the series holds.
func (s *SeriesString) Type() string {
	return "string"
}

// NRows returns how many rows the series contains.
func (s *SeriesString) NRows(options ...Options) int {
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
func (s *SeriesString) Value(row int, options ...Options) interface{} {
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
func (s *SeriesString) ValueString(row int, options ...Options) string {
	return s.valFormatter(s.Value(row, options...))
}

// Prepend is used to set a value to the beginning of the
// series. val can be a concrete data type or nil. Nil
// represents the absence of a value.
func (s *SeriesString) Prepend(val interface{}, options ...Options) {
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
func (s *SeriesString) Append(val interface{}, options ...Options) int {
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
func (s *SeriesString) Insert(row int, val interface{}, options ...Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.insert(row, val)
}

func (s *SeriesString) insert(row int, val interface{}) {
	s.values = append(s.values, nil)
	copy(s.values[row+1:], s.values[row:])

	if s.valToPointer(val) == nil {
		s.nilCount++
	}

	s.values[row] = s.valToPointer(val)
}

// Remove is used to delete the value of a particular row.
func (s *SeriesString) Remove(row int, options ...Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	if s.values[row] == nil {
		s.nilCount--
	}
	s.values = append(s.values[:row], s.values[row+1:]...)
}

// Update is used to update the value of a particular row.
// val can be a concrete data type or nil. Nil represents
// the absence of a value.
func (s *SeriesString) Update(row int, val interface{}, options ...Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	if s.values[row] == nil && s.valToPointer(val) != nil {
		s.nilCount--
	}
	if s.values[row] != nil && s.valToPointer(val) == nil {
		s.nilCount++
	}
	s.values[row] = s.valToPointer(val)
}

func (s *SeriesString) valToPointer(v interface{}) *string {
	switch val := v.(type) {
	case nil:
		return nil
	case *string:
		if val == nil {
			return nil
		}
		return &[]string{*val}[0]
	case string:
		return &val
	default:
		_ = v.(string) // Intentionally panic
		return nil
	}
}

// SetValueToStringFormatter is used to set a function
// to convert the value of a particular row to a string
// representation.
func (s *SeriesString) SetValueToStringFormatter(f ValueToStringFormatter) {
	if f == nil {
		s.valFormatter = DefaultValueFormatter
		return
	}
	s.valFormatter = f
}

// Swap is used to swap 2 values based on their row position.
func (s *SeriesString) Swap(row1, row2 int, options ...Options) {
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
func (s *SeriesString) IsEqualFunc(a, b interface{}) bool {

	if a == nil {
		if b == nil {
			return true
		} else {
			return false
		}
	} else {
		if b == nil {
			return false
		} else {
			s1 := a.(string)
			s2 := b.(string)

			return s1 == s2
		}
	}

}

// IsLessThanFunc returns true if a is less than b.
func (s *SeriesString) IsLessThanFunc(a, b interface{}) bool {

	if a == nil {
		if b == nil {
			return true
		} else {
			return true
		}
	} else {
		if b == nil {
			return false
		} else {
			s1 := a.(string)
			s2 := b.(string)

			return s1 < s2
		}
	}

}

// Sort will sort the series.
func (s *SeriesString) Sort(options ...Options) {

	var sortDesc bool

	if len(options) == 0 {
		s.lock.Lock()
		defer s.lock.Unlock()
	} else {
		if !options[0].DontLock {
			s.lock.Lock()
			defer s.lock.Unlock()
		}
		sortDesc = options[0].SortDesc
	}

	sort.SliceStable(s.values, func(i, j int) (ret bool) {
		defer func() {
			if sortDesc {
				ret = !ret
			}
		}()

		if s.values[i] == nil {
			if s.values[j] == nil {
				// both are nil
				return true
			} else {
				return true
			}
		} else {
			if s.values[j] == nil {
				// i has value and j is nil
				return false
			} else {
				// Both are not nil
				ti := *s.values[i]
				tj := *s.values[j]

				return ti < tj
			}
		}
	})
}

// Lock will lock the Series allowing you to directly manipulate
// the underlying slice with confidence.
func (s *SeriesString) Lock() {
	s.lock.Lock()
}

// Unlock will unlock the Series that was previously locked.
func (s *SeriesString) Unlock() {
	s.lock.Unlock()
}

// Copy will create a new copy of the series.
// It is recommended that you lock the Series before attempting
// to Copy.
func (s *SeriesString) Copy(r ...Range) Series {

	if len(s.values) == 0 {
		return &SeriesString{
			valFormatter: s.valFormatter,
			name:         s.name,
			values:       []*string{},
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

	return &SeriesString{
		valFormatter: s.valFormatter,
		name:         s.name,
		values:       newSlice,
		nilCount:     s.nilCount,
	}
}

// Table will produce the Series in a table.
func (s *SeriesString) Table(r ...Range) string {

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
			sVals := []string{fmt.Sprintf("%d:", row), s.ValueString(row, Options{true, false})}
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
func (s *SeriesString) String() string {
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
			out = out + s.ValueString(row, Options{true, false}) + " "
		}
		return out + "]"
	} else {
		for row := range s.values {
			out = out + s.ValueString(row, Options{true, false}) + " "
		}
		return out + "]"
	}
}

// ContainsNil will return True or false
// True if there are any Nil value
// False if there are none
func (s *SeriesString) ContainsNil() bool {

	return s.nilCount > 0
}
