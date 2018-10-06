package dataframe

// SeriesInit is used to configure the series
// when it is initialized
type SeriesInit struct {
	// Prefill the series with nil ("NaN") with
	// Size number of rows.
	Size int
	// How much memory to preallocate.
	// If you know the size of the series in advance,
	// it is better to preallocate the capacity of the
	// underlying slice.
	Capacity int
}
