package funcs

import (
	"context"
	"errors"
	"math"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/sandertv/go-formula/v2"
)

type ApplyFunctionOptions struct {

	// CustomFns adds custom functions to be used by fn.
	// See: https://godoc.org/github.com/Sandertv/go-formula/v2#Formula.Func
	CustomFns map[string]func(args ...float64) float64

	// DontLock can be set to true if the Series should not be locked.
	DontLock bool
}

type Func struct {
	Fn     string
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
	return nil, &dataframe.RowError{row, errors.New("undefined")}
}

func ApplyFunction(ctx context.Context, sdf interface{}, fn []Func, opts ...ApplyFunctionOptions) error {
	switch typ := sdf.(type) {
	case dataframe.Series:

	case *dataframe.DataFrame:
		err := applyFunctionDataFrame(ctx, typ, fn, opts...)
		return err
	default:
		panic("sdf must be a Series or DataFrame")
	}

	return nil
}

func applyFunctionDataFrame(ctx context.Context, df *dataframe.DataFrame, fn []Func, opts ...ApplyFunctionOptions) error {
	if len(opts) > 0 {
		if !opts[0].DontLock {
			df.Lock()
			defer df.Unlock()
		}
	}

	n := df.NRows(dataframe.DontLock)

	// Parse fn
	formulas := pfs{}

	for _, v := range fn {
		x, err := formula.New(v.Fn)
		if err != nil {
			return err
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
	for row := 0; row < n; row++ {
		f, err := formulas.pf(row)
		if err != nil {
			return err
		}

		variables := []formula.Variable{}
		vals := df.Row(row, true, dataframe.SeriesName)
		for k, v := range vals {
			if v == nil {
				variables = append(variables, formula.Var(k.(string), math.NaN()))
			} else {
				variables = append(variables, formula.Var(k.(string), v.(float64)))
			}
		}

		rval := f.Eval(variables...)
		df.Update(row, 2, rval, dataframe.DontLock)
	}

	return nil
}
