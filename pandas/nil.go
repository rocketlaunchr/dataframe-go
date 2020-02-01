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

// FillNil replaces all nil values with replaceVal. When applied to a DataFrame, replaceVal must be of type
// map[interface{}]interface{}, where the key is the Series name or Series index.
//
// Note: Not all Series recognise the type of replaceVal. The function will panic on such a scenario.
// A string is recognised by all built-in Series types.
func FillNil(ctx context.Context, replaceVal interface{}, sdf interface{}, lock bool) error {

	switch typ := sdf.(type) {
	case dataframe.Series:
		return fillNilSeries(ctx, replaceVal, typ, lock)
	case *dataframe.DataFrame:
		return fillNilDataFrame(ctx, replaceVal.(map[interface{}]interface{}), typ, lock)
	default:
		panic("sdf must be a Series or DataFrame")
	}
}

func fillNilSeries(ctx context.Context, replaceVal interface{}, s dataframe.Series, lock bool) error {

	if lock {
		s.Lock()
		defer s.Unlock()
	}

	fn := dataframe.ApplySeriesFn(func(val interface{}, row, nRows int) interface{} {
		if val == nil {
			return replaceVal
		}
		return val
	})

	opts := dataframe.FilterOptions{
		InPlace:  true,
		DontLock: true,
	}

	_, err := dataframe.Apply(ctx, s, fn, opts)
	return err
}

func fillNilDataFrame(ctx context.Context, replaceVal map[interface{}]interface{}, df *dataframe.DataFrame, lock bool) error {

	if lock {
		df.Lock()
		defer df.Unlock()
	}

	fn := dataframe.ApplyDataFrameFn(func(vals map[interface{}]interface{}, row, nRows int) map[interface{}]interface{} {

		nilFound := false

		out := map[interface{}]interface{}{}

		for k, val := range vals {
			if val == nil {
				_, found := replaceVal[k]
				if found {
					nilFound = true
					out[k] = replaceVal[k]
				}
			}
		}

		if nilFound {
			return out
		}
		return nil
	})

	opts := dataframe.FilterOptions{
		InPlace:  true,
		DontLock: true,
	}

	_, err := dataframe.Apply(ctx, df, fn, opts)
	return err
}
