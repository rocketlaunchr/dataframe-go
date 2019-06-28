// Copyright 2019 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package utils

import (
	"context"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

type common interface {
	Lock()
	Unlock()
	NRows(options ...dataframe.Options) int
	Swap(row1, row2 int, options ...dataframe.Options)
}

func Reverse(ctx context.Context, s common, r ...dataframe.Range) error {

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

	for i := rRows/2 - 1; i >= 0; i-- {
		opp := rRows - 1 - i

		s.Swap(i+start, opp+start, dataframe.DontLock)
	}

	return nil
}
