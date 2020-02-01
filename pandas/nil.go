// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package pandas

import (
	"context"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// DropNil drops all rows that contain nil values.
func DropNil(ctx context.Context, sdf interface{}, lock bool) error {

	switch typ := sdf.(type) {
	case dataframe.Series:
		return dropNilSeries(ctx, typ, lock)
	case *dataframe.DataFrame:
		return dropNilDataFrame(ctx, typ, lock)
	default:
		panic("sdf must be a Series or DataFrame")
	}
}

func dropNilSeries(ctx context.Context, s dataframe.Series, lock bool) error {

	if lock {
		s.Lock()
		defer s.Unlock()
	}

	fn := dataframe.FilterSeriesFn(func(val interface{}, row, nRows int) (dataframe.FilterAction, error) {
		if val == nil {
			return dataframe.DROP, nil
		}
		return dataframe.KEEP, nil
	})

	opts := dataframe.FilterOptions{
		InPlace:  true,
		DontLock: true,
	}

	_, err := dataframe.Filter(ctx, s, fn, opts)
	return err
}

func dropNilDataFrame(ctx context.Context, df *dataframe.DataFrame, lock bool) error {

	if lock {
		df.Lock()
		defer df.Unlock()
	}

	fn := dataframe.FilterDataFrameFn(func(vals map[interface{}]interface{}, row, nRows int) (dataframe.FilterAction, error) {

		for _, val := range vals {
			if val == nil {
				return dataframe.DROP, nil
			}
		}
		return dataframe.KEEP, nil
	})

	opts := dataframe.FilterOptions{
		InPlace:  true,
		DontLock: true,
	}

	_, err := dataframe.Filter(ctx, df, fn, opts)
	return err
}
