package dataframe

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
)

type SeriesFloat64 struct {
	valFormatter ValueToStringFormatter

	lock   sync.RWMutex
	name   string
	Values []*float64
}

// NewSeriesFloat64 creates a new series with the underlying type as float64
func NewSeriesFloat64(name string, init *SeriesInit, vals ...interface{}) *SeriesFloat64 {
	s := &SeriesFloat64{
		name:   name,
		Values: []*float64{},
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

	s.Values = make([]*float64, size, capacity)
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

func (s *SeriesFloat64) Name() string {
	return s.name
}

func (s *SeriesFloat64) Type() string {
	return "float64"
}

func (s *SeriesFloat64) NRows(options ...Options) int {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return len(s.Values)
}

func (s *SeriesFloat64) Value(row int, options ...Options) interface{} {
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

func (s *SeriesFloat64) ValueString(row int, options ...Options) string {
	return s.valFormatter(s.Value(row, options...))
}

func (s *SeriesFloat64) Prepend(val interface{}, options ...Options) {
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

func (s *SeriesFloat64) Append(val interface{}, options ...Options) int {
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

func (s *SeriesFloat64) Insert(row int, val interface{}, options ...Options) {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.insert(row, val)
}

func (s *SeriesFloat64) insert(row int, val interface{}) {
	s.Values = append(s.Values, nil)
	copy(s.Values[row+1:], s.Values[row:])
	s.Values[row] = s.valToPointer(val)
}

func (s *SeriesFloat64) Remove(row int, options ...Options) {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values = append(s.Values[:row], s.Values[row+1:]...)
}

func (s *SeriesFloat64) Update(row int, val interface{}, options ...Options) {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values[row] = s.valToPointer(val)
}

func (s *SeriesFloat64) valToPointer(v interface{}) *float64 {
	if v == nil {
		return nil
	} else {
		f, err := strconv.ParseFloat(fmt.Sprintf("%v", v), 64)
		if err != nil {
			_ = v.(float64)
		}
		return &f
	}
}

func (s *SeriesFloat64) SetValueToStringFormatter(f ValueToStringFormatter) {
	if f == nil {
		s.valFormatter = DefaultValueFormatter
		return
	}
	s.valFormatter = f
}

func (s *SeriesFloat64) Swap(row1, row2 int, options ...Options) {
	if row1 == row2 {
		return
	}

	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values[row1], s.Values[row2] = s.Values[row2], s.Values[row1]
}

func (s *SeriesFloat64) IsEqualFunc(a, b interface{}) bool {
	f1 := a.(float64)
	f2 := b.(float64)

	return f1 == f2
}

func (s *SeriesFloat64) IsLessThanFunc(a, b interface{}) bool {
	f1 := a.(float64)
	f2 := b.(float64)

	return f1 < f2
}

func (s *SeriesFloat64) Sort(options ...Options) {

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
			} else {
				return true
			}
		} else {
			if s.Values[j] == nil {
				// i has value and j is nil
				return false
			} else {
				// Both are not nil
				ti := *s.Values[i]
				tj := *s.Values[j]

				return ti < tj
			}
		}
	})
}

func (s *SeriesFloat64) Lock() {
	s.lock.Lock()
}

func (s *SeriesFloat64) Unlock() {
	s.lock.Unlock()
}
