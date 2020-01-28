// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package pandas

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"

	"gonum.org/v1/gonum/stat"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

type DescribeOutput struct {
	Count       []int
	NilCount    []int
	Median      []float64
	Mean        []float64
	StdDev      []float64
	Min         []float64
	Max         []float64
	Percentiles [][]float64

	series      bool
	percentiles []float64
}

func (do DescribeOutput) String() string {

	out := map[string]interface{}{}

	if do.series {
		out["count"] = do.Count[0]
		out["nil count"] = do.NilCount[0]

		if len(do.Median) > 0 {
			out["median"] = do.Median[0]
		} else {
			out["median"] = nil
		}

		if len(do.Mean) > 0 {
			out["mean"] = do.Mean[0]
		} else {
			out["mean"] = nil
		}

		if len(do.StdDev) > 0 {
			out["std dev"] = do.StdDev[0]
		} else {
			out["std dev"] = nil
		}

		if len(do.Min) > 0 {
			out["min"] = do.Min[0]
		} else {
			out["min"] = nil
		}

		if len(do.Max) > 0 {
			out["max"] = do.Max[0]
		} else {
			out["max"] = nil
		}

		for i, p := range do.percentiles {
			out[strconv.FormatFloat(100*p, 'f', -1, 64)+"%"] = do.Percentiles[0][i]
		}
	}

	return printMap(out)
}

type DescribeOptions struct {
	Percentiles []float64
	Whitelist   []interface{}
	Blacklist   []interface{}
}

func Describe(ctx context.Context, s interface{}, opts ...DescribeOptions) (DescribeOutput, error) {

	switch _s := s.(type) {
	case dataframe.Series:
		return describeSeries(ctx, _s, opts...)
		// case *dataframe.DataFrame:
	}

	panic(fmt.Sprintf("interface conversion: %T is not a valid Series or DataFrame", s))
}

func describeSeries(ctx context.Context, s dataframe.Series, opts ...DescribeOptions) (DescribeOutput, error) {

	if len(opts) == 0 {
		opts = append(opts, DescribeOptions{
			Percentiles: []float64{.2, .4, .6, .8},
		})
	} else {
		if opts[0].Percentiles == nil {
			opts[0].Percentiles = []float64{.2, .4, .6, .8}
		}
	}

	out := DescribeOutput{
		series:      true,
		percentiles: opts[0].Percentiles,

		Count:    []int{s.NRows()},
		NilCount: []int{s.NilCount()},
	}

	var (
		sf        *dataframe.SeriesFloat64
		floatable bool
	)

	if sf64, ok := s.(*dataframe.SeriesFloat64); ok {
		sf = sf64
		floatable = true
	} else {
		_, floatable = s.(dataframe.ToSeriesFloat64)
		if floatable {
			var err error
			sf, err = s.(dataframe.ToSeriesFloat64).ToSeriesFloat64(ctx, false)
			if err != nil {
				return DescribeOutput{}, err
			}
		}
	}

	if floatable {
		var vals []float64

		// Arrange values from lowest to highest
		for _, v := range sf.Values {
			if !math.IsNaN(v) {
				vals = append(vals, v)
			}
		}
		sort.Float64s(vals)

		// Median
		out.Median = []float64{stat.Quantile(0.5, stat.Empirical, vals, nil)}

		// Mean
		out.Mean = []float64{stat.Mean(vals, nil)}

		// Std Dev
		out.StdDev = []float64{stat.StdDev(vals, nil)}

		// Percentiles
		out.Percentiles = append(out.Percentiles, []float64{})
		for _, p := range opts[0].Percentiles {
			q := stat.Quantile(p, stat.Empirical, vals, nil)
			out.Percentiles[len(out.Percentiles)-1] = append(out.Percentiles[len(out.Percentiles)-1], q)
		}

		if len(vals) > 0 {
			out.Min = []float64{vals[0]}
			out.Max = []float64{vals[len(vals)-1]}
		}
	}

	return out, nil
}
