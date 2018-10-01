package dataframe

import ()

type Options struct {
	// Don't apply lock
	DontLock bool

	// Sort in descending order
	SortDesc bool
}

type ValueToStringFormatter func(val interface{}) string

type Series interface {

	// Name returns the series name
	Name() string

	// Type returns the type of data the series holds
	Type() string

	// NRows returns how many rows the series contains
	NRows(options ...Options) int

	// Value returns the value of a particular row.
	// The return value could be nil or the concrete type
	// the data type held by the series.
	// Pointers are never returned.
	Value(row int, options ...Options) interface{}

	// ValueString returns a string representation of a
	// particular row. The string representation is defined
	// by the function set in SetValueToStringFormatter.
	// By default, a nil value is returned as "NaN".
	ValueString(row int, options ...Options) string

	// Prepend is used to set a value to the beginning of the
	// series. val can be a concrete data type or nil. Nil
	// represents the absence of a value.
	Prepend(val interface{}, options ...Options)

	// Append is used to set a value to the end of the series.
	// val can be a concrete data type or nil. Nil represents
	// the absence of a value.
	Append(val interface{}, options ...Options) int

	// Insert is used to set a value at an arbitary row in
	// the series. All existing values from that row onwards
	// are shifted by 1. val can be a concrete data type or nil.
	// Nil represents the absence of a value.
	Insert(row int, val interface{}, options ...Options)

	// Remove is used to delete the value of a particular row.
	Remove(row int, options ...Options)

	// Update is used to update the value of a particular row.
	// val can be a concrete data type or nil. Nil represents
	// the absence of a value.
	Update(row int, val interface{}, options ...Options)

	// SetValueToStringFormatter is used to set a function
	// to convert the value of a particular row to a string
	// representation.
	SetValueToStringFormatter(f ValueToStringFormatter)

	// Sort will sort the series
	Sort(options ...Options)

	// SortLessFunc is a function that returns true when val1 is
	// less than val2. val1 and val2 must not be nil.
	// See: https://golang.org/pkg/sort/#SliceStable
	SortLessFunc() func(val1 interface{}, val2 interface{}) bool

	// SortEqualFunc is a function that returns true when val1 is
	// equal to val2. val1 and val2 must not be nil.
	SortEqualFunc() func(val1 interface{}, val2 interface{}) bool

	// Lock will lock the Series allowing you to directly manipulate
	// the underlying slice with confidence.
	Lock()

	// Unlock will unlock the Series that was previously locked.
	Unlock()

	// Swap is used to swap 2 values based on their row position.
	Swap(row1, row2 int, options ...Options)
}
