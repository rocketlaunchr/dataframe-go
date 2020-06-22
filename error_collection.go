// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"fmt"
	"sync"
)

// ErrorCollection is used to hold multiple errors.
type ErrorCollection struct {
	sync.Mutex
	errors []error
}

// NewErrorCollection returns a new ErrorCollection.
// ErrorCollection is compatible with errors.Is and
// errors.As functions.
func NewErrorCollection() *ErrorCollection {
	return &ErrorCollection{}
}

// AddError inserts a new error to the ErrorCollection.
// If err is nil, the function will panic.
func (ec *ErrorCollection) AddError(err error, lock ...bool) {
	if err == nil {
		panic("error must not be nil")
	}
	if len(lock) == 0 || lock[0] == true {
		// default
		ec.Lock()
		defer ec.Unlock()
	}
	ec.errors = append(ec.errors, err)
}

// IsNil returns whether the ErrorCollection contains any errors.
func (ec *ErrorCollection) IsNil(lock ...bool) bool {
	if len(lock) == 0 || lock[0] == true {
		// default
		ec.Lock()
		defer ec.Unlock()
	}
	return len(ec.errors) == 0
}

// Error implements the error interface.
func (ec *ErrorCollection) Error() string {
	ec.Lock()
	defer ec.Unlock()
	var out string
	for i, err := range ec.errors {
		if i != len(ec.errors)-1 {
			out = out + fmt.Sprintf("%v\n", err)
		} else {
			out = out + fmt.Sprintf("%v", err)
		}
	}
	return out
}
