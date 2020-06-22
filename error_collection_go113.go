// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

// +build go1.13

package dataframe

import (
	"errors"
)

// Is returns true if ErrorCollection contains err.
func (ec *ErrorCollection) Is(err error) bool {
	ec.Lock()
	defer ec.Unlock()
	if err == nil && len(ec.errors) == 0 {
		return true
	}
	for _, v := range ec.errors {
		if errors.Is(v, err) {
			return true
		}
	}
	return false
}

// As returns true if ErrorCollection contains an error
// of the same type as target. If so, it will set target
// to that error.
func (ec *ErrorCollection) As(target interface{}) bool {
	ec.Lock()
	defer ec.Unlock()
	if target == nil {
		panic("errors: target cannot be nil")
	}
	for _, v := range ec.errors {
		if errors.As(v, target) {
			return true
		}
	}
	return false
}
