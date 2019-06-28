// Copyright 2019 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package utils

import (
	"context"
	"math/rand"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

type shuffler interface {
	Lock()
	Unlock()
	NRows(options ...dataframe.Options) int
	Swap(row1, row2 int, options ...dataframe.Options)
}

// Shuffle will randomly shuffle the rows in a Dataframe or Series.
// If a Range is provided, only the rows within the range are shuffled.
// s will be locked for the duration of the operation.
// The function will panic if s is not a Dataframe or Series.
//
// WARNING: Context cancelation may cause a panic in some circumstances.
func Shuffle(ctx context.Context, s shuffler, r ...dataframe.Range) error {

	s.Lock()
	defer s.Unlock()

	if len(r) == 0 {
		r = append(r, dataframe.Range{})
	}

	nRows := s.NRows(dataframe.DontLock)
	if nRows == 0 {
		return nil
	}

	start, end, err := r[0].Limits(nRows)
	if err != nil {
		return err
	}

	if start == 0 && end == nRows-1 {
		rand.Shuffle(nRows, func(i, j int) {
			if err := ctx.Err(); err != nil {
				panic(err)
			}
			s.Swap(i, j, dataframe.DontLock)
		})
		return nil
	}

	var cpy shuffler

	switch t := s.(type) {
	case dataframe.Series:
		cpy = t.Copy(r[0])
	case *dataframe.DataFrame:
		cpy = t.Copy(r[0])
	default:
		panic("s is not a dataframe or series")
	}

	rand.Shuffle(cpy.NRows(dataframe.DontLock), func(i, j int) {
		if err := ctx.Err(); err != nil {
			panic(err)
		}
		cpy.Swap(i, j, dataframe.DontLock)
	})

	for i, j := start, 0; i < end+1; i, j = i+1, j+1 {
		if err := ctx.Err(); err != nil {
			return err
		}

		switch t := s.(type) {
		case dataframe.Series:
			t.Update(i, cpy.(dataframe.Series).Value(j, dataframe.DontLock), dataframe.DontLock)
		case *dataframe.DataFrame:
			for k, s := range cpy.(*dataframe.DataFrame).Series {
				val := s.Value(j, dataframe.DontLock)
				t.Update(i, k, val, dataframe.DontLock)
			}
		}

	}

	return nil
}
