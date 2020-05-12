package funcs

import (
	"context"
	"errors"
	"golang.org/x/xerrors"
	"math"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/sandertv/go-formula/v2"
)

// PiecewiseFuncOptions modifies the behaviour of the PiecewiseFunc function.
type PiecewiseFuncOptions struct {

	// CustomFns adds custom functions to be used by Fn.
	// See: https://godoc.org/github.com/Sandertv/go-formula/v2#Formula.Func
	CustomFns map[string]func(args ...float64) float64

	// DontLock can be set to true if the DataFrame should not be locked.
	DontLock bool

	// Range is used to limit which rows the PiecewiseFuncDefn gets applied to.
	Range *dataframe.Range
}

// ErrUndefined indicates that the PiecewiseFuncDefn's domain is not defined for a given row.
var ErrUndefined = errors.New("undefined")

// PiecewiseFuncDefn represents a piecewise function.
// A piecewise function is a function that is defined on a sequence of intervals.
//
// See: https://mathworld.wolfram.com/PiecewiseFunction.html
//
// Example:
//
//  fn := []funcs.SubFunc{
//     {
//        Fn:     "sin(x)+2*y",
//        Domain: &[]dataframe.Range{dataframe.RangeFinite(0, 2)}[0],
//     },
//     {
//        Fn:     "0",
//        Domain: nil,
//     },
//  }
//
type PiecewiseFuncDefn []SubFunc

// SubFunc represents a function that makes up a subset of the piecewise function.
type SubFunc struct {

	// Fn is a string representing the function. It must be accepted by https://godoc.org/github.com/Sandertv/go-formula/v2#New.
	// The variables used in Fn must correspond to the Series' names in the DataFrame. Custom functions can be defined and added
	// using the options.
	//
	// Example: "sin(x)+2*y"
	//
	Fn string

	// Domain of Fn based on DataFrame's rows.
	Domain *dataframe.Range
}

type parsedF struct {
	f *formula.Formula
	s int
	e int
}

type pfs []parsedF

func (p pfs) pf(row int) (*formula.Formula, error) {
	for _, v := range p {
		if row >= v.s && row <= v.e {
			return v.f, nil
		}
	}
	return nil, &dataframe.RowError{row, ErrUndefined}
}

// PiecewiseFunc applies a PiecewiseFuncDefn to a particular series in a DataFrame.
// Consult funcs_test.go for usage.
func PiecewiseFunc(ctx context.Context, df *dataframe.DataFrame, fn PiecewiseFuncDefn, col interface{}, opts ...PiecewiseFuncOptions) error {

	var ss dataframe.Series

	switch C := col.(type) {
	case dataframe.Series:
		ss = C
	case int:
		ss = df.Series[C]
	case string:
		ss = df.Series[df.MustNameToColumn(C, dataframe.DontLock)]
	}

	var r dataframe.Range
	if len(opts) > 0 {
		if !opts[0].DontLock {
			df.Lock()
			defer df.Unlock()
		}

		if opts[0].Range != nil {
			r = *opts[0].Range
		}
	}

	n := df.NRows(dataframe.DontLock)
	s, e, err := r.Limits(n)
	if err != nil {
		return err
	}

	// Parse fn
	formulas := pfs{}

	for _, v := range fn {
		x, err := formula.New(v.Fn)
		if err != nil {
			switch serr := err.(type) {
			case xerrors.Wrapper:
				xerr := serr.Unwrap()
				if xerr != nil {
					return xerrors.Errorf("error parsing Fn: \"%s\" err: %w", v.Fn, xerr)
				}
				return xerrors.Errorf("error parsing Fn: \"%s\" err: %w", v.Fn, err)
			default:
				return xerrors.Errorf("error parsing Fn: \"%s\" err: %w", v.Fn, err)
			}
		}

		// Add custom functions
		if len(opts) > 0 {
			for k, v := range opts[0].CustomFns {
				x.Func(k, 0, v)
			}
		}

		if v.Domain == nil {
			formulas = append(formulas, parsedF{x, 0, n - 1})
		} else {
			s, e, err := v.Domain.Limits(n)
			if err != nil {
				return err
			}
			formulas = append(formulas, parsedF{x, s, e})
		}
	}

	// Iterate over each row
	for row := s; row <= e; row++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		f, err := formulas.pf(row)
		if err != nil {
			return err
		}

		variables := []formula.Variable{}
		vals := df.Row(row, true, dataframe.SeriesName)
		for k, v := range vals {
			switch v.(type) {
			case nil:
				variables = append(variables, formula.Var(k.(string), math.NaN()))
			case float64:
				variables = append(variables, formula.Var(k.(string), v.(float64)))
			}
		}

		rval := f.Eval(variables...)
		ss.Update(row, rval, dataframe.DontLock)
	}

	return nil
}
