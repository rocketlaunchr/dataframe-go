// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package forecast

import (
	"context"
	"fmt"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"golang.org/x/sync/errgroup"
)

func interpolateDataFrame(ctx context.Context, df *dataframe.DataFrame, opts InterpolateOptions) ([]*dataframe.OrderedMapIntFloat64, error) {
	if !opts.DontLock {
		df.Lock()
		defer df.Unlock()
	}

	omaps := make([]*dataframe.OrderedMapIntFloat64, len(df.Series))

	g, newCtx := errgroup.WithContext(ctx)

	for i := range df.Series {
		g.Go(func() error {
			data, ok := df.Series[i].(*dataframe.SeriesFloat64)
			if !ok {
				return fmt.Errorf("column [%d] not a valid series float64 column", i)
			}

			omap, err := Interpolate(newCtx, data, opts)
			if err != nil {
				return err
			}

			if !opts.InPlace {
				omaps[i] = omap.(*dataframe.OrderedMapIntFloat64)
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
