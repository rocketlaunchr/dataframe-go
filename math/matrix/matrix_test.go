package matrix

import (
	"context"
	"testing"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

func TestTranspose(t *testing.T) {

	s1 := dataframe.NewSeriesFloat64("0", nil, 1, 2)
	s2 := dataframe.NewSeriesFloat64("1", nil, 3, 4)
	s3 := dataframe.NewSeriesFloat64("2", nil, 5, 6)
	df := dataframe.NewDataFrame(s1, s2, s3)

	// Transpose df and transpose again to get the same matrix
	mw := MatrixWrap{df}
	nmw := mw.T().T()

	eq, err := mw.DataFrame.IsEqual(context.Background(), nmw.(MatrixWrap).DataFrame)
	if err != nil {
		t.Errorf("wrong err: expected: %v got: %v", nil, err)
	}

	if !eq {
		t.Errorf("matrix transpose error")
	}
}
