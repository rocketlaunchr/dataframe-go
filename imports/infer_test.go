// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestInferSeries(t *testing.T) {
	is := newInferSeries("x", &[]int{8}[0])

	is.add("1")
	is.add("2")
	is.add("3.")
	is.add("4")
	is.add("5")
	is.add("6")
	is.add("7")
	is.add("8")

	spew.Dump(is)

	s, _ := is.inferred()
	spew.Dump(s)
}
