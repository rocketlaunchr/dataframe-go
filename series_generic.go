// Copyright 2018 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/olekukonko/tablewriter"
)

// SeriesGeneric is a series of data where the contained data can be
// of any type. Only concrete data types can be used.
type SeriesGeneric struct {
	valFormatter   ValueToStringFormatter
	isEqualFunc    IsEqualFunc
	isLessThanFunc IsLessThanFunc

	concreteType interface{} // The underlying data type

	lock   sync.RWMutex
	name   string
	values []interface{}
}

// NewSeries creates a new generic series.
func NewSeries(name string, concreteType interface{}, init *SeriesInit, vals ...interface{}) *SeriesGeneric {

	// Validate concrete type
	err := checkConcreteType(concreteType)
	if err != nil {
		panic(err)
	}

	s := &SeriesGeneric{
		isEqualFunc:  DefaultIsEqualFunc,
		name:         name,
		concreteType: concreteType,
		values:       []interface{}{},
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

	s.values = make([]interface{}, size, capacity)
	s.valFormatter = DefaultValueFormatter

	for idx, v := range vals {
		if v != nil {
			if err := s.checkValue(v); err != nil {
				panic(err)
			}
		}

		if idx < size {
			s.values[idx] = v
		} else {
			s.values = append(s.values, v)
		}
	}

	return s
}

// Name returns the series name.
func (s *SeriesGeneric) Name() string {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.name
}

// Rename renames the series.
func (s *SeriesGeneric) Rename(n string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.name = n
}

// Type returns the type of data the series holds.
func (s *SeriesGeneric) Type() string {
	return fmt.Sprintf("%T", s.concreteType)
}

// NRows returns how many rows the series contains.
func (s *SeriesGeneric) NRows(options ...Options) int {
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
func (s *SeriesGeneric) Value(row int, options ...Options) interface{} {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	val := s.values[row]
	if val == nil {
		return nil
	}
	return val
}

// ValueString returns a string representation of a
// particular row. The string representation is defined
// by the function set in SetValueToStringFormatter.
// By default, a nil value is returned as "NaN".
func (s *SeriesGeneric) ValueString(row int, options ...Options) string {
	return s.valFormatter(s.Value(row, options...))
}

// Prepend is used to set a value to the beginning of the
// series. val can be a concrete data type or nil. Nil
// represents the absence of a value.
func (s *SeriesGeneric) Prepend(val interface{}, options ...Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	// See: https://stackoverflow.com/questions/41914386/what-is-the-mechanism-of-using-append-to-prepend-in-go

	if cap(s.values) > len(s.values) {
		// There is already extra capacity so copy current values by 1 spot
		s.values = s.values[:len(s.values)+1]
		copy(s.values[1:], s.values)
		if val == nil {
			s.values[0] = nil
		} else {
			if err := s.checkValue(val); err != nil {
				panic(err)
			}
			s.values[0] = val
		}
		return
	}

	// No room, new slice needs to be allocated:
	s.insert(0, val)
}

// Append is used to set a value to the end of the series.
// val can be a concrete data type or nil. Nil represents
// the absence of a value.
func (s *SeriesGeneric) Append(val interface{}, options ...Options) int {
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
func (s *SeriesGeneric) Insert(row int, val interface{}, options ...Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.insert(row, val)
}

func (s *SeriesGeneric) insert(row int, val interface{}) {
	s.values = append(s.values, nil)
	copy(s.values[row+1:], s.values[row:])

	if val == nil {
		s.values[row] = nil
	} else {
		if err := s.checkValue(val); err != nil {
			panic(err)
		}
		s.values[row] = val
	}
}

// Remove is used to delete the value of a particular row.
func (s *SeriesGeneric) Remove(row int, options ...Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.values = append(s.values[:row], s.values[row+1:]...)
}

// Update is used to update the value of a particular row.
// val can be a concrete data type or nil. Nil represents
// the absence of a value.
func (s *SeriesGeneric) Update(row int, val interface{}, options ...Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	if val == nil {
		s.values[row] = nil
	} else {
		if err := s.checkValue(val); err != nil {
			panic(err)
		}
		s.values[row] = val
	}
}

// SetValueToStringFormatter is used to set a function
// to convert the value of a particular row to a string
// representation.
func (s *SeriesGeneric) SetValueToStringFormatter(f ValueToStringFormatter) {
	if f == nil {
		s.valFormatter = DefaultValueFormatter
		return
	}
	s.valFormatter = f
}

// IsEqualFunc returns true if a is equal to b.
func (s *SeriesGeneric) IsEqualFunc(a, b interface{}) bool {

	if s.isEqualFunc == nil {
		panic(errors.New("IsEqualFunc not set"))
	}

	return s.isEqualFunc(a, b)
}

// IsLessThanFunc returns true if a is less than b.
func (s *SeriesGeneric) IsLessThanFunc(a, b interface{}) bool {

	if s.isLessThanFunc == nil {
		panic(errors.New("IsEqualFunc not set"))
	}

	return s.isLessThanFunc(a, b)
}

// SetIsEqualFunc sets a function which can be used to determine
// if 2 values in the series are equal.
func (s *SeriesGeneric) SetIsEqualFunc(f IsEqualFunc) {
	if f == nil {
		// Return to default
		s.isEqualFunc = DefaultIsEqualFunc
	} else {
		s.isEqualFunc = f
	}
}

// SetIsLessThanFunc sets a function which can be used to determine
// if a value is less than another in the series.
func (s *SeriesGeneric) SetIsLessThanFunc(f IsLessThanFunc) {
	if f == nil {
		// Return to default
		s.isLessThanFunc = nil
	} else {
		s.isLessThanFunc = f
	}
}

// Sort will sort the series.
func (s *SeriesGeneric) Sort(options ...Options) {

	if s.isLessThanFunc == nil {
		panic(fmt.Errorf("cannot sort without setting IsLessThanFunc"))
	}

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

		left := s.values[i]
		right := s.values[j]

		if left == nil {
			if right == nil {
				// both are nil
				return true
			}
			return true
		}

		if right == nil {
			// left has value and right is nil
			return false
		}
		// Both are not nil
		return s.isLessThanFunc(left, right)
	})
}

// Swap is used to swap 2 values based on their row position.
func (s *SeriesGeneric) Swap(row1, row2 int, options ...Options) {
	if row1 == row2 {
		return
	}

	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.values[row1], s.values[row2] = s.values[row2], s.values[row1]
}

// Lock will lock the Series allowing you to directly manipulate
// the underlying slice with confidence.
func (s *SeriesGeneric) Lock() {
	s.lock.Lock()
}

// Unlock will unlock the Series that was previously locked.
func (s *SeriesGeneric) Unlock() {
	s.lock.Unlock()
}

// Copy will create a new copy of the series.
// It is recommended that you lock the Series before attempting
// to Copy.
func (s *SeriesGeneric) Copy(r ...Range) Series {

	if len(s.values) == 0 {
		return &SeriesGeneric{
			valFormatter:   s.valFormatter,
			isEqualFunc:    s.isEqualFunc,
			isLessThanFunc: s.isLessThanFunc,

			concreteType: s.concreteType,

			name:   s.name,
			values: []interface{}{},
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

	return &SeriesGeneric{
		valFormatter:   s.valFormatter,
		isEqualFunc:    s.isEqualFunc,
		isLessThanFunc: s.isLessThanFunc,

		concreteType: s.concreteType,

		name:   s.name,
		values: newSlice,
	}
}

// Table will produce the Series in a table.
func (s *SeriesGeneric) Table(r ...Range) string {

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
func (s *SeriesGeneric) String() string {
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
	}

	for row := range s.values {
		out = out + s.ValueString(row, Options{true, false}) + " "
	}
	return out + "]"
}
