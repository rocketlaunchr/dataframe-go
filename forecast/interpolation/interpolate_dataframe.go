// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package forecast

import (
	"context"
	"golang.org/x/sync/errgroup"
	"sync"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func interpolateDataFrame(ctx context.Context, df *dataframe.DataFrame, opts InterpolateOptions) (map[interface{}]*dataframe.OrderedMapIntFloat64, error) {
	if !opts.DontLock {
		df.Lock()
		defer df.Unlock()
	}

	var lock sync.Mutex
	omaps := map[interface{}]*dataframe.OrderedMapIntFloat64{}

	if opts.HorizAxis != nil {
		switch s := opts.HorizAxis.(type) {
		case int:
			opts.HorizAxis = df.Series[s]
		case string:
			i, err := df.NameToColumn(s, dataframe.DontLock)
			if err != nil {
				return nil, err
			}
			opts.HorizAxis = df.Series[i]
		case dataframe.Series:

		default:
			panic("HorizAxis option must be a SeriesFloat64/SeriesTime or convertable to a SeriesFloat64")
		}
	}

	g, newCtx := errgroup.WithContext(ctx)

	for i := range df.Series {
		i := i
		if df.Series[i] == opts.HorizAxis {
			continue
		}

		fs, ok := df.Series[i].(*dataframe.SeriesFloat64)
		if !ok {
			continue
		}

		g.Go(func() error {
			omap, err := Interpolate(newCtx, fs, opts)
			if err != nil {
				return err
			}

			if !opts.InPlace {
				lock.Lock()
				omaps[i] = omap.(*dataframe.OrderedMapIntFloat64)
				omaps[df.Series[i].Name()] = omap.(*dataframe.OrderedMapIntFloat64)
				lock.Unlock()
			}

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return nil, err
	}

	if opts.InPlace {
		return nil, nil
	}
	return omaps, nil
}
