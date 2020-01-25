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
func NewErrorCollection() *ErrorCollection {
	return &ErrorCollection{}
}

// AddError inserts a new error to the ErrorCollection.
func (ec *ErrorCollection) AddError(err error, lock ...bool) {
	if len(lock) == 0 || lock[0] == true {
		// default
		ec.Lock()
		defer ec.Unlock()
	}

	ec.errors = append(ec.errors, err)
}

// IsNil return whether the ErrorCollection contains any errors.
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

	var out string

	for _, err := range ec.errors {
		out = out + fmt.Sprintf("%v\n", err)
	}

	return out
}
