// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package pandas

import (
	"context"
	"math"
	"sort"

	"gonum.org/v1/gonum/stat"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func describeSeries(ctx context.Context, s dataframe.Series, opts ...DescribeOptions) (DescribeOutput, error) {

	out := DescribeOutput{
		percentiles: opts[0].Percentiles,
		headers:     []string{s.Name()},

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
			if err != nil && sf == nil {
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
		func() {
			defer func() {
				if x := recover(); x != nil {
					out.Median = []float64{math.NaN()}
				}
			}()
			out.Median = []float64{stat.Quantile(0.5, stat.Empirical, vals, nil)}
		}()

		// Mean
		out.Mean = []float64{stat.Mean(vals, nil)}

		// Std Dev
		out.StdDev = []float64{stat.StdDev(vals, nil)}

		// Percentiles
		out.Percentiles = append(out.Percentiles, []float64{})
		for _, p := range opts[0].Percentiles {
			func() {
				defer func() {
					if x := recover(); x != nil {
						out.Percentiles[len(out.Percentiles)-1] = append(out.Percentiles[len(out.Percentiles)-1], math.NaN())
					}
				}()
				q := stat.Quantile(p, stat.Empirical, vals, nil)
				out.Percentiles[len(out.Percentiles)-1] = append(out.Percentiles[len(out.Percentiles)-1], q)
			}()
		}

		if len(vals) > 0 {
			out.Min = []float64{vals[0]}
			out.Max = []float64{vals[len(vals)-1]}
		}
	}

	return out, nil
}
