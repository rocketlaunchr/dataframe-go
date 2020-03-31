package dataframe

import (
	"testing"
	"time"
)

func TestOrderedMapIntFloat64(t *testing.T) {
	omapIf := NewOrderedMapIntFloat64(false)
	nomapIf := NewOrderedMapIntFloat64(true)

	data := []*OrderedMapIntFloat64{omapIf, nomapIf}

	for i := range data {
		od := data[i]
		// Set values
		od.Set(2, 45.7)
		od.Set(5, 65.34)
		od.Set(3, 26.54)
		od.Set(4, 23.57)
		od.Set(1, 98.14)
		od.Set(6, 16.54)

		// Values Iterator
		iterator := od.ValuesIterator()

		for {
			key, val := iterator()

			if key == nil {
				break
			}

			v, found := od.Get(*key)
			if !found {
				t.Errorf("error: get value [%f] of key [%d] is expected to be available", val, key)
			} else {
				if v != val {
					t.Errorf("error: actual recovered value [%f] is not same as expected value [%f]", v, val)
				}
			}
		}
		// Test Delete
		od.Delete(4)

	}
}

func TestOrderedMapIntMixed(t *testing.T) {
	omapIm := NewOrderedMapIntMixed(false)
	nomapIm := NewOrderedMapIntMixed(true)

	data := []*OrderedMapIntMixed{omapIm, nomapIm}

	for i := range data {
		od := data[i]
		// Set values
		od.Set(6, 45.7)
		od.Set(2, "22.5")
		od.Set(5, "silly")
		od.Set(1, 65)
		od.Set(3, time.Now())
		od.Set(4, 16.54)

		// Values Iterator
		iterator := od.ValuesIterator()

		for {
			key, val := iterator()

			if key == nil {
				break
			}

			v, found := od.Get(*key)
			if !found {
				t.Errorf("error: get value [%v] [%T] of key [%d] is expected to be available", val, val, key)
			} else {
				if v != val {
					t.Errorf("error: actual recovered value [%v] [%T] is not same as expected value [%f]", v, v, val)
				}
			}
		}
		// Test Delete
		od.Delete(3)

	}
}
