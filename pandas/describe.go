// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package pandas

import (
	"context"
	"fmt"
	"strconv"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// DescribeOutput contains statistical data for a DataFrame or Series.
// It is not intended to be perused. Use fmt package to view data.
type DescribeOutput struct {
	Count       []int
	NilCount    []int
	Median      []float64
	Mean        []float64
	StdDev      []float64
	Min         []float64
	Max         []float64
	Percentiles [][]float64

	percentiles []float64
	headers     []string
}

// String implements the Stringer interface in fmt package.
func (do DescribeOutput) String() string {

	out := map[string][]interface{}{}

	for idx := range do.headers {
		out["count"] = append(out["count"], do.Count[idx])
		out["nil count"] = append(out["nil count"], do.NilCount[idx])

		if len(do.Median) > 0 {
			out["median"] = append(out["median"], do.Median[idx])
		} else {
			out["median"] = append(out["median"], "NaN")
		}

		if len(do.Mean) > 0 {
			out["mean"] = append(out["mean"], do.Mean[idx])
		} else {
			out["mean"] = append(out["mean"], "NaN")
		}

		if len(do.StdDev) > 0 {
			out["std dev"] = append(out["std dev"], do.StdDev[idx])
		} else {
			out["std dev"] = append(out["std dev"], "NaN")
		}

		if len(do.Min) > 0 {
			out["min"] = append(out["min"], do.Min[idx])
		} else {
			out["min"] = append(out["min"], "NaN")
		}

		if len(do.Max) > 0 {
			out["max"] = append(out["max"], do.Max[idx])
		} else {
			out["max"] = append(out["max"], "NaN")
		}

		for i, p := range do.percentiles {
			key := strconv.FormatFloat(100*p, 'f', -1, 64) + "%"
			out[key] = append(out[key], do.Percentiles[idx][i])
		}
	}

	return printMap(do.headers, out)
}

// DescribeOptions configures what Describe should return or display.
type DescribeOptions struct {

	// Percentiles sets which Quantiles to return.
	Percentiles []float64

	// Whitelist sets which Series to provided statistics for.
	Whitelist []interface{}

	// Blacklist sets which Series to NOT provide statistics for.
	Blacklist []interface{}
}

// Describe outputs various statistical information a Series or Dataframe.
//
// See: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.describe.html#pandas.DataFrame.describe
func Describe(ctx context.Context, s interface{}, opts ...DescribeOptions) (DescribeOutput, error) {

	if len(opts) == 0 {
		opts = append(opts, DescribeOptions{
			Percentiles: []float64{.2, .4, .6, .8},
		})
	} else {
		if opts[0].Percentiles == nil {
			opts[0].Percentiles = []float64{.2, .4, .6, .8}
		}
	}

	switch _s := s.(type) {
	case dataframe.Series:
		return describeSeries(ctx, _s, opts...)
	case *dataframe.DataFrame:
		return describeDataframe(ctx, _s, opts...)
	}

	panic(fmt.Sprintf("interface conversion: %T is not a valid Series or DataFrame", s))
}
