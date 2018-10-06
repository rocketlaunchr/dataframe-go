package dataframe

import (
	"errors"
	"fmt"
	"sort"
	"sync"
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
	Values []interface{}
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
		Values:       []interface{}{},
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

	s.Values = make([]interface{}, size, capacity)
	s.valFormatter = DefaultValueFormatter

	for idx, v := range vals {
		if v != nil {
			if err := s.checkValue(v); err != nil {
				panic(err)
			}
		}

		if idx < size {
			s.Values[idx] = v
		} else {
			s.Values = append(s.Values, v)
		}
	}

	return s
}

func (s *SeriesGeneric) Name() string {
	return s.name
}

func (s *SeriesGeneric) Type() string {
	return fmt.Sprintf("%T", s.concreteType)
}

func (s *SeriesGeneric) NRows(options ...Options) int {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return len(s.Values)
}

func (s *SeriesGeneric) Value(row int, options ...Options) interface{} {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	val := s.Values[row]
	if val == nil {
		return nil
	}
	return val
}

func (s *SeriesGeneric) ValueString(row int, options ...Options) string {
	return s.valFormatter(s.Value(row, options...))
}

func (s *SeriesGeneric) Prepend(val interface{}, options ...Options) {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	// See: https://stackoverflow.com/questions/41914386/what-is-the-mechanism-of-using-append-to-prepend-in-go

	if cap(s.Values) > len(s.Values) {
		// There is already extra capacity so copy current values by 1 spot
		s.Values = s.Values[:len(s.Values)+1]
		copy(s.Values[1:], s.Values)
		if val == nil {
			s.Values[0] = nil
		} else {
			if err := s.checkValue(val); err != nil {
				panic(err)
			}
			s.Values[0] = val
		}
		return
	}

	// No room, new slice needs to be allocated:
	s.insert(0, val)
}

func (s *SeriesGeneric) Append(val interface{}, options ...Options) int {
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

func (s *SeriesGeneric) Insert(row int, val interface{}, options ...Options) {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.insert(row, val)
}

func (s *SeriesGeneric) insert(row int, val interface{}) {
	s.Values = append(s.Values, nil)
	copy(s.Values[row+1:], s.Values[row:])

	if val == nil {
		s.Values[row] = nil
	} else {
		if err := s.checkValue(val); err != nil {
			panic(err)
		}
		s.Values[row] = val
	}
}

func (s *SeriesGeneric) Remove(row int, options ...Options) {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values = append(s.Values[:row], s.Values[row+1:]...)
}

func (s *SeriesGeneric) Update(row int, val interface{}, options ...Options) {
	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	if val == nil {
		s.Values[row] = nil
	} else {
		if err := s.checkValue(val); err != nil {
			panic(err)
		}
		s.Values[row] = val
	}
}

func (s *SeriesGeneric) SetValueToStringFormatter(f ValueToStringFormatter) {
	if f == nil {
		s.valFormatter = DefaultValueFormatter
		return
	}
	s.valFormatter = f
}

func (s *SeriesGeneric) IsEqualFunc(a, b interface{}) bool {

	if s.isEqualFunc == nil {
		panic(errors.New("IsEqualFunc not set"))
	}

	return s.isEqualFunc(a, b)
}

func (s *SeriesGeneric) IsLessThanFunc(a, b interface{}) bool {

	if s.isLessThanFunc == nil {
		panic(errors.New("IsEqualFunc not set"))
	}

	return s.isLessThanFunc(a, b)
}

func (s *SeriesGeneric) SetIsEqualFunc(f IsEqualFunc) {
	if f == nil {
		// Return to default
		s.isEqualFunc = DefaultIsEqualFunc
	} else {
		s.isEqualFunc = f
	}
}

func (s *SeriesGeneric) SetIsLessThanFunc(f IsLessThanFunc) {
	if f == nil {
		// Return to default
		s.isLessThanFunc = nil
	} else {
		s.isLessThanFunc = f
	}
}

func (s *SeriesGeneric) Sort(options ...Options) {

	if s.isLessThanFunc == nil {
		panic(fmt.Errorf("cannot sort without setting IsLessThanFunc"))
	}

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

		left := s.Values[i]
		right := s.Values[j]

		if left == nil {
			if right == nil {
				// both are nil
				return true
			} else {
				return true
			}
		} else {
			if right == nil {
				// left has value and right is nil
				return false
			} else {
				// Both are not nil
				return s.isLessThanFunc(left, right)
			}
		}
	})
}

func (s *SeriesGeneric) Swap(row1, row2 int, options ...Options) {
	if row1 == row2 {
		return
	}

	if len(options) > 0 && !options[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values[row1], s.Values[row2] = s.Values[row2], s.Values[row1]
}

func (s *SeriesGeneric) Lock() {
	s.lock.Lock()
}

func (s *SeriesGeneric) Unlock() {
	s.lock.Unlock()
}
