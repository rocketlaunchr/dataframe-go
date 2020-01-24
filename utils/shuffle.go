// Copyright 2019-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

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

// ShuffleOptions
type ShuffleOptions struct {

	// R is used to limit the range of the Series for search purposes.
	R *dataframe.Range

	// DontLock can be set to true if the series should not be locked.
	DontLock bool
}

// Shuffle will randomly shuffle the rows in a Dataframe or Series.
// If a Range is provided, only the rows within the range are shuffled.
// s will be locked for the duration of the operation.
func Shuffle(ctx context.Context, s common, opts ...ShuffleOptions) (rErr error) {

	defer func() {
		if x := recover(); x != nil {
			rErr = x.(error)
		}
	}()

	if len(opts) == 0 {
		opts = append(opts, ShuffleOptions{R: &dataframe.Range{}})
	} else if opts[0].R == nil {
		opts[0].R = &dataframe.Range{}
	}

	if !opts[0].DontLock {
		s.Lock()
		defer s.Unlock()
	}

	nRows := s.NRows(dataframe.DontLock)
	if nRows == 0 {
		return nil
	}

	start, _, err := opts[0].R.Limits(nRows)
	if err != nil {
		return err
	}

	rRows, _ := opts[0].R.NRows(nRows)

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
