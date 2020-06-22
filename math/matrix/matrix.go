package matrix

import (
	"strconv"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

// Matrix replicates gonum/mat Matrix interface.
type Matrix interface {
	// Dims returns the dimensions of a Matrix.
	Dims() (r, c int)

	// At returns the value of a matrix element at row i, column j.
	// It will panic if i or j are out of bounds for the matrix.
	At(i, j int) float64

	// T returns the transpose of the Matrix. Whether T returns a copy of the
	// underlying data is implementation dependent.
	// This method may be implemented using the Transpose type, which
	// provides an implicit matrix transpose.
	T() Matrix
}

// MatrixWrap is used to wrap a dataframe so that it can satisfy the Matrix interface.
// All Series contained by DataFrame must be of type SeriesFloat64.
type MatrixWrap struct {
	*dataframe.DataFrame
}

// Dims returns the dimensions of a Matrix.
func (m MatrixWrap) Dims() (r, c int) {
	return m.NRows(dataframe.DontLock), len(m.Series)
}

// At returns the value of a matrix element at row i, column j.
// It will panic if i or j are out of bounds for the matrix.
func (m MatrixWrap) At(i, j int) float64 {
	col := m.Series[j]
	return col.Value(i, dataframe.DontLock).(float64)
}

// T returns the transpose of the MatrixWrap. It returns a copy instead of performing
// the operation "in-place".
func (m MatrixWrap) T() Matrix {
	// More direct approach: https://gist.github.com/tanaikech/5cb41424ff8be0fdf19e78d375b6adb8

	mm, nn := m.Dims()

	// Create new dataframe
	ss := []dataframe.Series{}
	init := &dataframe.SeriesInit{Size: nn}

	for i := 0; i < mm; i++ {
		ss = append(ss, dataframe.NewSeriesFloat64(strconv.Itoa(i), init))
	}

	df := dataframe.NewDataFrame(ss...)

	// Copy values into df
	for i := 0; i < mm; i++ {
		vals := m.Row(i, true, dataframe.SeriesIdx)
		for k, v := range vals {
			df.Series[i].Update(k.(int), v, dataframe.DontLock)
		}
	}

	return MatrixWrap{df}
}

// Set alters the matrix element at row i, column j to v.
// It will panic if i or j are out of bounds for the matrix.
func (m MatrixWrap) Set(i, j int, v float64) {
	m.Update(i, j, v, dataframe.DontLock)
}
