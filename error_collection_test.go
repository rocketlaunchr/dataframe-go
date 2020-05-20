// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"errors"
	"testing"
)

var (
	ErrTestA = errors.New("test A error")
	ErrTestB = errors.New("test B error")
	ErrTestC = errors.New("test C error")
)

func TestErrorCollection(t *testing.T) {

	ec := NewErrorCollection()

	// Issue: https://github.com/golang/go/issues/39167
	// if !errors.Is(ec, nil) {
	// 	t.Errorf("errors.Is(ec, nil) should return true")
	// }

	ec.AddError(ErrTestA)
	ec.AddError(ErrTestB)

	// Test nil
	isNil := ec.IsNil()
	if isNil == true {
		t.Errorf("err collection should not be nil")
	}

	// Test items in Collection
	resA := ec.Is(ErrTestA)
	resB := ec.Is(ErrTestB)
	resC := ec.Is(ErrTestC)

	if resA == false {
		t.Errorf("err collection should contain ErrTestA")
	}

	if resB == false {
		t.Errorf("err collection should contain ErrTestB")
	}

	if resC == true {
		t.Errorf("err collection should not contain ErrTestC")
	}
}
