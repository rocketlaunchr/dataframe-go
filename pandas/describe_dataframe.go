// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package pandas

import (
	"context"
	"fmt"
	"golang.org/x/sync/errgroup"
	"math"
	"sync"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func describeDataframe(ctx context.Context, df *dataframe.DataFrame, opts ...DescribeOptions) (DescribeOutput, error) {

	out := DescribeOutput{
		percentiles: opts[0].Percentiles,
	}

	// Compile whitelist and blacklist
	wl := map[int]struct{}{}
	bl := map[int]struct{}{}

	for _, v := range opts[0].Whitelist {
		switch _v := v.(type) {
		case int:
			wl[_v] = struct{}{}
		case string:
			idx, err := df.NameToColumn(_v)
			if err != nil {
				continue
			}
			wl[idx] = struct{}{}
		default:
			panic(fmt.Errorf("unknown whitelist item: %v", _v))
		}
	}

	for _, v := range opts[0].Blacklist {
		switch _v := v.(type) {
		case int:
			bl[_v] = struct{}{}
		case string:
			idx, err := df.NameToColumn(_v)
			if err != nil {
				continue
			}
			bl[idx] = struct{}{}
		default:
			panic(fmt.Errorf("unknown blacklist item: %v", _v))
		}
	}

	idxs := []int{}
	g, newCtx := errgroup.WithContext(ctx)
	var lock sync.Mutex
	los := map[int]DescribeOutput{}

	for idx, s := range df.Series {
		idx := idx

		// Check whitelist
		if _, exists := wl[idx]; exists || opts[0].Whitelist == nil {
			// Now check blacklist
			if _, exists := bl[idx]; !exists || opts[0].Blacklist == nil {

				idxs = append(idxs, idx)

				// Accept this Series
				out.headers = append(out.headers, s.Name())

				g.Go(func() error {

					lo, err := describeSeries(newCtx, df.Series[idx], opts[0])
					if err != nil {
						return err
					}

					lock.Lock()
					los[idx] = lo
					lock.Unlock()
					return nil
				})
			}
		}
	}

	err := g.Wait()
	if err != nil {
		return DescribeOutput{}, err
	}

	// Compile results together
	for _, idx := range idxs {
		ldo := los[idx]

		out.Count = append(out.Count, ldo.Count[0])
		out.NilCount = append(out.NilCount, ldo.NilCount[0])

		if len(ldo.Median) > 0 {
			out.Median = append(out.Median, ldo.Median[0])
		} else {
			out.Median = append(out.Median, math.NaN())
		}

		if len(ldo.Mean) > 0 {
			out.Mean = append(out.Mean, ldo.Mean[0])
		} else {
			out.Mean = append(out.Mean, math.NaN())
		}

		if len(ldo.StdDev) > 0 {
			out.StdDev = append(out.StdDev, ldo.StdDev[0])
		} else {
			out.StdDev = append(out.StdDev, math.NaN())
		}

		if len(ldo.Min) > 0 {
			out.Min = append(out.Min, ldo.Min[0])
		} else {
			out.Min = append(out.Min, math.NaN())
		}

		if len(ldo.Max) > 0 {
			out.Max = append(out.Max, ldo.Max[0])
		} else {
			out.Max = append(out.Max, math.NaN())
		}

		if len(ldo.Percentiles) > 0 {
			out.Percentiles = append(out.Percentiles, ldo.Percentiles[0])
		} else {
			out.Percentiles = append(out.Percentiles, []float64{})
		}
	}

	return out, nil
}
