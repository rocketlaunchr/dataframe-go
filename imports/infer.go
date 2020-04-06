// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"context"
	"strconv"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

var timelayouts = []string{
	time.ANSIC,
	time.UnixDate,
	time.RubyDate,
	time.RFC822,
	time.RFC822Z,
	time.RFC850,
	time.RFC1123,
	time.RFC1123Z,
	time.RFC3339,
	time.RFC3339Nano,
	time.Kitchen,
	time.Stamp,
	time.StampMilli,
	time.StampMicro,
	time.StampNano,
	"2006-01-02",
	"2006-01-02 15:04:05",
}

type inferSeries struct {
	series    []dataframe.Series
	knownSize *int
	initCap   int
	added     int
}

func newInferSeries(name string, knownSize *int) *inferSeries {

	initCap := 5
	if knownSize != nil {
		initCap2 := int(0.05 * float64(*knownSize))
		if initCap2 > initCap {
			initCap = initCap2
		}
	}
	init := &dataframe.SeriesInit{Capacity: initCap}

	is := &inferSeries{knownSize: knownSize, initCap: initCap}

	is.series = []dataframe.Series{}

	// Create initial set of series
	is.series = append(is.series, dataframe.NewSeriesFloat64(name, init))
	is.series = append(is.series, dataframe.NewSeriesInt64(name, init))
	for _, layout := range timelayouts {
		ts := dataframe.NewSeriesTime(name, init)
		ts.Layout = layout
		is.series = append(is.series, ts)
	}
	is.series = append(is.series, dataframe.NewSeriesString(name, init))

	return is
}

// 	We are only appending in here
func (is *inferSeries) Insert(row int, val interface{}, opts ...dataframe.Options) {

	// val can be nil or string

	if len(is.series) == 0 {
		return
	}

	is.added++

	if is.knownSize != nil && is.added == is.initCap+1 {
		// Create a new series with full capacity and then copy old values over

		init := &dataframe.SeriesInit{Capacity: *is.knownSize}

		for i, s := range is.series {

			var ns dataframe.Series
			iterator := s.ValuesIterator(dataframe.ValuesOptions{0, 1, true})

			switch x := s.(type) {
			case *dataframe.SeriesFloat64:
				ns = dataframe.NewSeriesFloat64(x.Name(dataframe.DontLock), init)

				for {
					row, val, _ := iterator()
					if row == nil {
						break
					}
					ns.Append(val, dataframe.DontLock)
				}
			case *dataframe.SeriesInt64:
				ns = dataframe.NewSeriesInt64(x.Name(dataframe.DontLock), init)

				for {
					row, val, _ := iterator()
					if row == nil {
						break
					}
					ns.Append(val, dataframe.DontLock)
				}
			case *dataframe.SeriesString:
				ns = dataframe.NewSeriesString(x.Name(dataframe.DontLock), init)

				for {
					row, val, _ := iterator()
					if row == nil {
						break
					}
					ns.Append(val, dataframe.DontLock)
				}
			case *dataframe.SeriesTime:
				ns = dataframe.NewSeriesTime(x.Name(dataframe.DontLock), init)

				for {
					row, val, _ := iterator()
					if row == nil {
						break
					}
					ns.Append(val, dataframe.DontLock)
				}
			}

			// Replace old series with new series
			is.series[i] = ns
		}
	}

	toRemove := []int{}

	for i := range is.series {
		s := is.series[i]

		if val == nil {
			s.Append(nil, dataframe.DontLock)
			continue
		}

		// val is string from here onwards

		switch x := s.(type) {
		case *dataframe.SeriesFloat64:
			f, err := strconv.ParseFloat(val.(string), 64)
			if err != nil {
				toRemove = append(toRemove, i)
			} else {
				x.Values = append(x.Values, f)
			}
		case *dataframe.SeriesInt64:
			valStr := val.(string)

			if valStr == "true" || valStr == "TRUE" || valStr == "True" {
				s.Append(int64(1), dataframe.DontLock)
			} else if valStr == "false" || valStr == "FALSE" || valStr == "False" {
				s.Append(int64(0), dataframe.DontLock)
			} else {
				f, err := strconv.ParseInt(valStr, 10, 64)
				if err != nil {
					toRemove = append(toRemove, i)
				} else {
					s.Append(f, dataframe.DontLock)
				}
			}
		case *dataframe.SeriesString:
			s.Append(val, dataframe.DontLock)
		case *dataframe.SeriesTime:
			t, err := time.Parse(x.Layout, val.(string))
			if err != nil {
				toRemove = append(toRemove, i)
			} else {
				x.Values = append(x.Values, &t)
			}
		}
	}

	// Remove series
	for i := len(toRemove) - 1; i >= 0; i-- {
		idx := toRemove[i]
		is.series = append(is.series[:idx], is.series[idx+1:]...)
	}

}

func (is *inferSeries) inferred() (dataframe.Series, bool) {
	if len(is.series) == 0 {
		return nil, false
	} else if len(is.series) == 1 {
		return is.series[0], false
	}

	// We have multiple possible series. Which one do we pick?

	// Do we have a SeriesInt64
	for _, s := range is.series {
		if is, ok := s.(*dataframe.SeriesInt64); ok {
			// We found a SeriesInt64
			return is, true
		}
	}

	// Do we have a SeriesFloat64
	for _, s := range is.series {
		if fs, ok := s.(*dataframe.SeriesFloat64); ok {
			// We found a SeriesFloat64
			return fs, true
		}
	}

	// Do we have a SeriesTime
	for _, s := range is.series {
		if ts, ok := s.(*dataframe.SeriesTime); ok {
			// We found a SeriesTime
			return ts, true
		}
	}

	// Do we have a SeriesString
	for _, s := range is.series {
		if ss, ok := s.(*dataframe.SeriesString); ok {
			// We found a SeriesString
			return ss, true
		}
	}

	panic("should not reach here")
}

func (is *inferSeries) Name(opts ...dataframe.Options) string {
	return is.series[0].Name(dataframe.DontLock)
}

/* Stubs to satisfy dataframe.Series interface */

func (*inferSeries) Rename(n string, opts ...dataframe.Options) {}

func (*inferSeries) Type() string { return "" }

func (*inferSeries) NRows(opts ...dataframe.Options) int { return 0 }

func (*inferSeries) Value(row int, opts ...dataframe.Options) interface{} { return "" }

func (*inferSeries) ValueString(row int, opts ...dataframe.Options) string { return "" }

func (*inferSeries) Prepend(val interface{}, opts ...dataframe.Options) {}

func (*inferSeries) Append(val interface{}, opts ...dataframe.Options) int { return 0 }

func (*inferSeries) Remove(row int, opts ...dataframe.Options) {}

func (*inferSeries) Reset(opts ...dataframe.Options) {}

func (*inferSeries) ValuesIterator(opts ...dataframe.ValuesOptions) func() (*int, interface{}, int) {
	return nil
}

func (*inferSeries) Update(row int, val interface{}, opts ...dataframe.Options) {}

func (*inferSeries) SetValueToStringFormatter(f dataframe.ValueToStringFormatter) {}

func (*inferSeries) Sort(ctx context.Context, opts ...dataframe.SortOptions) (completed bool) {
	return false
}

func (*inferSeries) IsEqualFunc(a, b interface{}) bool { return false }

func (*inferSeries) IsLessThanFunc(a, b interface{}) bool { return false }

func (*inferSeries) Swap(row1, row2 int, opts ...dataframe.Options) {}

func (*inferSeries) Lock() {}

func (*inferSeries) Unlock() {}

func (*inferSeries) Copy(r ...dataframe.Range) dataframe.Series { return nil }

func (*inferSeries) ContainsNil(opts ...dataframe.Options) bool { return false }

func (*inferSeries) NilCount(opts ...dataframe.NilCountOptions) (int, error) { return 0, nil }

func (*inferSeries) IsEqual(ctx context.Context, s2 dataframe.Series, opts ...dataframe.IsEqualOptions) (bool, error) {
	return false, nil
}
