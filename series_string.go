package dataframe

import (
	"sort"
	"sync"
)

type SeriesString struct {
	valFormatter ValueToStringFormatter

	lock   sync.RWMutex
	name   string
	Values []*string
}

// NewSeriesString creates a new series with the underlying type as string
func NewSeriesString(name string, init *SeriesInit, vals ...interface{}) *SeriesString {
	s := &SeriesString{
		name:   name,
		Values: []*string{},
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

	s.Values = make([]*string, size, capacity)
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

func (s *SeriesString) Name() string {
	return s.name
}

func (s *SeriesString) Rename(n string) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	s.name = n
}

func (s *SeriesString) Type() string {
	return "string"
}

func (s *SeriesString) NRows(options ...Options) int {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return len(s.Values)
}

func (s *SeriesString) Value(row int, options ...Options) interface{} {
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

func (s *SeriesString) ValueString(row int, options ...Options) string {
	return s.valFormatter(s.Value(row, options...))
}

func (s *SeriesString) Prepend(val interface{}, options ...Options) {
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

func (s *SeriesString) Append(val interface{}, options ...Options) int {
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

func (s *SeriesString) Insert(row int, val interface{}, options ...Options) {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.insert(row, val)
}

func (s *SeriesString) insert(row int, val interface{}) {
	s.Values = append(s.Values, nil)
	copy(s.Values[row+1:], s.Values[row:])
	s.Values[row] = s.valToPointer(val)
}

func (s *SeriesString) Remove(row int, options ...Options) {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values = append(s.Values[:row], s.Values[row+1:]...)
}

func (s *SeriesString) Update(row int, val interface{}, options ...Options) {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values[row] = s.valToPointer(val)
}

func (s *SeriesString) valToPointer(v interface{}) *string {
	if v == nil {
		return nil
	} else {
		return &[]string{v.(string)}[0]
	}
}

func (s *SeriesString) SetValueToStringFormatter(f ValueToStringFormatter) {
	if f == nil {
		s.valFormatter = DefaultValueFormatter
		return
	}
	s.valFormatter = f
}

func (s *SeriesString) Swap(row1, row2 int, options ...Options) {
	if row1 == row2 {
		return
	}

	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values[row1], s.Values[row2] = s.Values[row2], s.Values[row1]
}

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

func (s *SeriesString) Sort(options ...Options) {

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

func (s *SeriesString) Lock() {
	s.lock.Lock()
}

func (s *SeriesString) Unlock() {
	s.lock.Unlock()
}
