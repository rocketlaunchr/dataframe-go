// Copyright 2019 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package series

import (
	"bytes"
	"fmt"
	"math/cmplx"
	"sort"
	"strings"
	"sync"

	"github.com/olekukonko/tablewriter"
	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// SeriesComplex128 is used for series containing complex128 data.
type SeriesComplex128 struct {
	valFormatter dataframe.ValueToStringFormatter

	lock sync.RWMutex
	name string
	// Values is exported to better improve interoperability with the gonum package.
	// See: https://godoc.org/gonum.org/v1/gonum
	Values   []complex128
	nilCount int
}

// NewSeriesComplex128 creates a new series with the underlying type as complex128
func NewSeriesComplex128(name string, init *dataframe.SeriesInit, vals ...interface{}) *SeriesComplex128 {
	s := &SeriesComplex128{
		name:     name,
		Values:   []complex128{},
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

	s.Values = make([]complex128, size, capacity) // Warning: filled with 0.0 (not NaN)
	s.valFormatter = DefaultValueFormatter

	for idx, v := range vals {
		val := s.valToPointer(v)
		if cmplx.IsNaN(val) {
			s.nilCount++
		}

		if idx < size {
			s.Values[idx] = val
		} else {
			s.Values = append(s.Values, val)
		}
	}

	if len(vals) < size {
		s.nilCount = s.nilCount + size - len(vals)
		// Fill with NaN
		for i := len(vals); i < size; i++ {
			s.Values[i] = cmplx.NaN()
		}
	}

	return s
}

// Name returns the series name.
func (s *SeriesComplex128) Name() string {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.name
}

// Rename renames the series.
func (s *SeriesComplex128) Rename(n string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.name = n
}

// Type returns the type of data the series holds.
func (s *SeriesComplex128) Type() string {
	return "complex128"
}

// NRows returns how many rows the series contains.
func (s *SeriesComplex128) NRows(options ...dataframe.Options) int {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return len(s.Values)
}

// Value returns the value of a particular row.
// The return value could be nil or the concrete type
// the data type held by the series.
// Pointers are never returned.
func (s *SeriesComplex128) Value(row int, options ...dataframe.Options) interface{} {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	val := s.Values[row]
	if cmplx.IsNaN(val) {
		return nil
	}
	return val
}

// ValueString returns a string representation of a
// particular row. The string representation is defined
// by the function set in SetValueToStringFormatter.
// By default, a nil value is returned as "NaN".
func (s *SeriesComplex128) ValueString(row int, options ...dataframe.Options) string {
	return s.valFormatter(s.Value(row, options...))
}

// Prepend is used to set a value to the beginning of the
// series. val can be a concrete data type or nil. Nil
// represents the absence of a value.
func (s *SeriesComplex128) Prepend(val interface{}, options ...dataframe.Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
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
func (s *SeriesComplex128) Append(val interface{}, options ...dataframe.Options) int {
	var locked bool
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
		locked = true
	}

	row := s.NRows(dataframe.Options{DontLock: locked})
	s.insert(row, val)
	return row
}

// Insert is used to set a value at an arbitrary row in
// the series. All existing values from that row onwards
// are shifted by 1. val can be a concrete data type or nil.
// Nil represents the absence of a value.
func (s *SeriesComplex128) Insert(row int, val interface{}, options ...dataframe.Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.insert(row, val)
}

func (s *SeriesComplex128) insert(row int, val interface{}) {
	s.Values = append(s.Values, cmplx.NaN())
	copy(s.Values[row+1:], s.Values[row:])

	v := s.valToPointer(val)
	if cmplx.IsNaN(v) {
		s.nilCount++
	}

	s.Values[row] = v
}

// Remove is used to delete the value of a particular row.
func (s *SeriesComplex128) Remove(row int, options ...dataframe.Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	if cmplx.IsNaN(s.Values[row]) {
		s.nilCount--
	}

	s.Values = append(s.Values[:row], s.Values[row+1:]...)
}

// Update is used to update the value of a particular row.
// val can be a concrete data type or nil. Nil represents
// the absence of a value.
func (s *SeriesComplex128) Update(row int, val interface{}, options ...dataframe.Options) {
	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	newVal := s.valToPointer(val)

	if cmplx.IsNaN(s.Values[row]) && !cmplx.IsNaN(newVal) {
		s.nilCount--
	} else if !cmplx.IsNaN(s.Values[row]) && cmplx.IsNaN(newVal) {
		s.nilCount++
	}

	s.Values[row] = newVal
}

func (s *SeriesComplex128) valToPointer(v interface{}) complex128 {
	switch val := v.(type) {
	case nil:
		return cmplx.NaN()
	case *complex128:
		if val == nil {
			return cmplx.NaN()
		}
		return *val
	case complex128:
		return val
	default:
		_ = v.(complex128) // Intentionally panic
		return 0
	}
}

// SetValueToStringFormatter is used to set a function
// to convert the value of a particular row to a string
// representation.
func (s *SeriesComplex128) SetValueToStringFormatter(f dataframe.ValueToStringFormatter) {
	if f == nil {
		s.valFormatter = DefaultValueFormatter
		return
	}
	s.valFormatter = f
}

// Swap is used to swap 2 values based on their row position.
func (s *SeriesComplex128) Swap(row1, row2 int, options ...dataframe.Options) {
	if row1 == row2 {
		return
	}

	if len(options) == 0 || (len(options) > 0 && !options[0].DontLock) {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values[row1], s.Values[row2] = s.Values[row2], s.Values[row1]
}

// IsEqualFunc returns true if a is equal to b.
func (s *SeriesComplex128) IsEqualFunc(a, b interface{}) bool {

	if a == nil {
		if b == nil {
			return true
		}
		return false
	}

	if b == nil {
		return false
	}
	f1 := a.(complex128)
	f2 := b.(complex128)

	return f1 == f2
}

// IsLessThanFunc returns true if a is less than b.
func (s *SeriesComplex128) IsLessThanFunc(a, b interface{}) bool {

	if a == nil {
		if b == nil {
			return true
		}
		return true
	}

	if b == nil {
		return false
	}
	f1 := a.(complex128)
	f2 := b.(complex128)

	return cmplx.Abs(f1) < cmplx.Abs(f2)
}

// Sort will sort the series.
func (s *SeriesComplex128) Sort(options ...dataframe.Options) {

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

	sort.SliceStable(s.Values, func(i, j int) (ret bool) {
		defer func() {
			if sortDesc {
				ret = !ret
			}
		}()

		if cmplx.IsNaN(s.Values[i]) {
			if cmplx.IsNaN(s.Values[j]) {
				// both are nil
				return true
			}
			return true
		}

		if cmplx.IsNaN(s.Values[j]) {
			// i has value and j is nil
			return false
		}
		// Both are not nil
		ti := s.Values[i]
		tj := s.Values[j]

		return cmplx.Abs(ti) < cmplx.Abs(tj)
	})
}

// Lock will lock the Series allowing you to directly manipulate
// the underlying slice with confidence.
func (s *SeriesComplex128) Lock() {
	s.lock.Lock()
}

// Unlock will unlock the Series that was previously locked.
func (s *SeriesComplex128) Unlock() {
	s.lock.Unlock()
}

// Copy will create a new copy of the series.
// It is recommended that you lock the Series before attempting
// to Copy.
func (s *SeriesComplex128) Copy(r ...dataframe.Range) dataframe.Series {

	if len(s.Values) == 0 {
		return &SeriesComplex128{
			valFormatter: s.valFormatter,
			name:         s.name,
			Values:       []complex128{},
			nilCount:     s.nilCount,
		}
	}

	if len(r) == 0 {
		r = append(r, dataframe.Range{})
	}

	start, end, err := r[0].Limits(len(s.Values))
	if err != nil {
		panic(err)
	}

	// Copy slice
	x := s.Values[start : end+1]
	newSlice := append(x[:0:0], x...)

	return &SeriesComplex128{
		valFormatter: s.valFormatter,
		name:         s.name,
		Values:       newSlice,
		nilCount:     s.nilCount,
	}
}

// Table will produce the Series in a table.
func (s *SeriesComplex128) Table(r ...dataframe.Range) string {

	s.lock.RLock()
	defer s.lock.RUnlock()

	if len(r) == 0 {
		r = append(r, dataframe.Range{})
	}

	data := [][]string{}

	headers := []string{"", s.name} // row header is blank
	footers := []string{fmt.Sprintf("%dx%d", len(s.Values), 1), s.Type()}

	if len(s.Values) > 0 {

		start, end, err := r[0].Limits(len(s.Values))
		if err != nil {
			panic(err)
		}

		for row := start; row <= end; row++ {
			sVals := []string{fmt.Sprintf("%d:", row), s.ValueString(row, dataframe.Options{true, false})}
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
func (s *SeriesComplex128) String() string {
	s.lock.RLock()
	defer s.lock.RUnlock()

	count := len(s.Values)

	out := "[ "

	if count > 6 {
		idx := []int{0, 1, 2, count - 3, count - 2, count - 1}
		for j, row := range idx {
			if j == 3 {
				out = out + "... "
			}
			out = out + s.ValueString(row, dataframe.Options{true, false}) + " "
		}
		return out + "]"
	}

	for row := range s.Values {
		out = out + s.ValueString(row, dataframe.Options{true, false}) + " "
	}
	return out + "]"

}

// ContainsNil will return whether or not the series contains any nil values.
func (s *SeriesComplex128) ContainsNil() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.nilCount > 0
}

// DefaultValueFormatter will return a string representation
// of the data in a particular row.
func DefaultValueFormatter(v interface{}) string {
	if v == nil {
		return "NaN"
	}
	return strings.TrimSuffix(strings.TrimPrefix(fmt.Sprintf("%v", v), "("), ")")
}
