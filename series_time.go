package dataframe

import (
	"sort"
	"sync"
	"time"
)

type SeriesTime struct {
	valFormatter ValueToStringFormatter

	lock   sync.RWMutex
	name   string
	Values []*time.Time
}

// NewSeriesTime creates a new series with the underlying type as time.Time
func NewSeriesTime(name string, init *SeriesInit, vals ...interface{}) *SeriesTime {
	s := &SeriesTime{
		name:   name,
		Values: []*time.Time{},
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
		if idx < size {
			s.Values[idx] = s.valToPointer(v)
		} else {
			s.Values = append(s.Values, s.valToPointer(v))
		}
	}

	return s
}

func (s *SeriesTime) Name() string {
	return s.name
}

func (s *SeriesTime) Rename(n string) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	s.name = n
}

func (s *SeriesTime) Type() string {
	return "time"
}

func (s *SeriesTime) NRows(options ...Options) int {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return len(s.Values)
}

func (s *SeriesTime) Value(row int, options ...Options) interface{} {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	val := s.Values[row]
	if val == nil {
		return nil
	}
	return *val
}

func (s *SeriesTime) ValueString(row int, options ...Options) string {
	return s.valFormatter(s.Value(row, options...))
}

func (s *SeriesTime) Prepend(val interface{}, options ...Options) {
	if len(options) > 0 && !options[0].DontLock {
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

func (s *SeriesTime) Append(val interface{}, options ...Options) int {
	var locked bool
	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
		locked = true
	}

	row := s.NRows(Options{DontLock: locked})
	s.insert(row, val)
	return row
}

func (s *SeriesTime) Insert(row int, val interface{}, options ...Options) {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.insert(row, val)
}

func (s *SeriesTime) insert(row int, val interface{}) {
	s.Values = append(s.Values, nil)
	copy(s.Values[row+1:], s.Values[row:])
	s.Values[row] = s.valToPointer(val)
}

func (s *SeriesTime) Remove(row int, options ...Options) {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values = append(s.Values[:row], s.Values[row+1:]...)
}

func (s *SeriesTime) Update(row int, val interface{}, options ...Options) {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values[row] = s.valToPointer(val)
}

func (s *SeriesTime) valToPointer(v interface{}) *time.Time {
	if v == nil {
		return nil
	}
	return &[]time.Time{v.(time.Time)}[0]
}

func (s *SeriesTime) SetValueToStringFormatter(f ValueToStringFormatter) {
	if f == nil {
		s.valFormatter = DefaultValueFormatter
		return
	}
	s.valFormatter = f
}

func (s *SeriesTime) Swap(row1, row2 int, options ...Options) {
	if row1 == row2 {
		return
	}

	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values[row1], s.Values[row2] = s.Values[row2], s.Values[row1]
}

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

func (s *SeriesTime) Sort(options ...Options) {

	var sortDesc bool

	if len(options) > 0 {
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
	})
}

func (s *SeriesTime) Lock() {
	s.lock.Lock()
}

func (s *SeriesTime) Unlock() {
	s.lock.Unlock()
}

func (s *SeriesTime) Copy(start interface{}, end interface{}) Series {

	if start == nil {
		start = 0
	} else {
		start = start.(int)
	}

	if end == nil {
		end = len(s.Values) - 1
	} else {
		end = end.(int)
	}

	// Copy slice
	x := s.Values[start.(int) : end.(int)+1]
	newSlice := append(x[:0:0], x...)

	return &SeriesTime{
		valFormatter: s.valFormatter,
		name:         s.name,
		Values:       newSlice,
	}
}
