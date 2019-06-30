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

// Shuffle will randomly shuffle the rows in a Dataframe or Series.
// If a Range is provided, only the rows within the range are shuffled.
// s will be locked for the duration of the operation.
func Shuffle(ctx context.Context, s common, r ...dataframe.Range) (rErr error) {

	defer func() {
		if x := recover(); x != nil {
			rErr = x.(error)
		}
	}()

	s.Lock()
	defer s.Unlock()

	if len(r) == 0 {
		r = append(r, dataframe.Range{})
	}

	nRows := s.NRows(dataframe.DontLock)
	if nRows == 0 {
		return nil
	}

	start, _, err := r[0].Limits(nRows)
	if err != nil {
		return err
	}

	rRows, _ := r[0].NRows(nRows)

	if rRows == 1 || rRows == 0 {
		return nil
	}

	rand.Shuffle(rRows, func(i, j int) {
		if err := ctx.Err(); err != nil {
			panic(err)
		}
		s.Swap(i+start, j+start, dataframe.DontLock)
	})

	return nil
}
