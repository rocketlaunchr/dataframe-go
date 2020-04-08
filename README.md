Dataframes for Go [![GoDoc](http://godoc.org/github.com/rocketlaunchr/dataframe-go?status.svg)](http://godoc.org/github.com/rocketlaunchr/dataframe-go) [![gocover.io](http://gocover.io/_badge/github.com/rocketlaunchr/dataframe-go)](https://gocover.io/github.com/rocketlaunchr/dataframe-go) [![Go Report Card](https://goreportcard.com/badge/github.com/rocketlaunchr/dataframe-go)](https://goreportcard.com/report/github.com/rocketlaunchr/dataframe-go)
===============

<p align="center">
<img src="https://github.com/rocketlaunchr/dataframe-go/raw/master/logo.png" alt="dataframe-go" />
</p>

Dataframes are used for statistics and data manipulation. You can think of a Dataframe as an excel spreadsheet.
This package is designed to be light-weight and intuitive.

The package is production ready but the API is not stable yet. Once stability is reached, version `1.0.0` will be tagged.
It is recommended your package manager locks to a commit id instead of the master branch directly.

⭐ **the project to show your appreciation.**

# Features

1. Importing from CSV, JSONL, MySQL & PostgreSQL
2. Exporting to CSV, JSONL, Excel, MySQL & PostgreSQL
3. Developer Friendly
4. Flexible - Create custom Series (custom data types)
5. Performant
6. Interoperability with [gonum package](https://godoc.org/gonum.org/v1/gonum).
7. [pandas sub-package](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html) ![Help Required](https://img.shields.io/badge/help-required-blueviolet)
8. Fake data generation
9. Interpolation (ForwardFill, BackwardFill, Linear, Spline, Lagrange)
10. Time-series Forecasting

# DataFrames

## Creating a DataFrame

```go

s1 := dataframe.NewSeriesInt64("day", nil, 1, 2, 3, 4, 5, 6, 7, 8)
s2 := dataframe.NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2, nil, nil, 84.2, 72, 89)
df := dataframe.NewDataFrame(s1, s2)

fmt.Print(df.Table())
  
OUTPUT:
+-----+-------+---------+
|     |  DAY  |  SALES  |
+-----+-------+---------+
| 0:  |   1   |  50.3   |
| 1:  |   2   |  23.4   |
| 2:  |   3   |  56.2   |
| 3:  |   4   |   NaN   |
| 4:  |   5   |   NaN   |
| 5:  |   6   |  84.2   |
| 6:  |   7   |   72    |
| 7:  |   8   |   89    |
+-----+-------+---------+
| 8X2 | INT64 | FLOAT64 |
+-----+-------+---------+

```

## Insert and Remove Row

```go

df.Append(nil, 9, 123.6)

df.Append(nil, map[string]interface{}{
	"day":   10,
	"sales": nil,
})

df.Remove(0)

OUTPUT:
+-----+-------+---------+
|     |  DAY  |  SALES  |
+-----+-------+---------+
| 0:  |   2   |  23.4   |
| 1:  |   3   |  56.2   |
| 2:  |   4   |   NaN   |
| 3:  |   5   |   NaN   |
| 4:  |   6   |  84.2   |
| 5:  |   7   |   72    |
| 6:  |   8   |   89    |
| 7:  |   9   |  123.6  |
| 8:  |  10   |   NaN   |
+-----+-------+---------+
| 9X2 | INT64 | FLOAT64 |
+-----+-------+---------+
```

## Update Row

```go

df.UpdateRow(0, nil, map[string]interface{}{
	"day":   3,
	"sales": 45,
})

```

## Sorting

```go

sks := []dataframe.SortKey{
	{Key: "sales", Desc: true},
	{Key: "day", Desc: true},
}

df.Sort(ctx, sks)

OUTPUT:
+-----+-------+---------+
|     |  DAY  |  SALES  |
+-----+-------+---------+
| 0:  |   9   |  123.6  |
| 1:  |   8   |   89    |
| 2:  |   6   |  84.2   |
| 3:  |   7   |   72    |
| 4:  |   3   |  56.2   |
| 5:  |   2   |  23.4   |
| 6:  |  10   |   NaN   |
| 7:  |   5   |   NaN   |
| 8:  |   4   |   NaN   |
+-----+-------+---------+
| 9X2 | INT64 | FLOAT64 |
+-----+-------+---------+
```
## Iterating

You can change the step and starting row. It may be wise to lock the DataFrame before iterating.

The returned values are a map containing the name of the series (`string`) and the order of the series (`int`).

```go

iterator := df.ValuesIterator(dataframe.ValuesOptions{0, 1, true}) // Don't apply read lock because we are write locking from outside.

df.Lock()
for {
	row, vals, _ := iterator()
	if row == nil {
		break
	}
	fmt.Println(*row, vals)
}
df.Unlock()

OUTPUT:
0 map[day:1 0:1 sales:50.3 1:50.3]
1 map[sales:23.4 1:23.4 day:2 0:2]
2 map[day:3 0:3 sales:56.2 1:56.2]
3 map[1:<nil> day:4 0:4 sales:<nil>]
4 map[day:5 0:5 sales:<nil> 1:<nil>]
5 map[sales:84.2 1:84.2 day:6 0:6]
6 map[day:7 0:7 sales:72 1:72]
7 map[day:8 0:8 sales:89 1:89]
```

## Statistics

You can easily calculate statistics for a Series using the [gonum](https://godoc.org/gonum.org/v1/gonum) or [montanaflynn/stats](https://godoc.org/github.com/montanaflynn/stats) package.

### Example

Generally, statistics packages require `SeriesFloat64` to operate. Some series provide easy conversion using the `ToSeriesFloat64` method.

```go
import "gonum.org/v1/gonum/stat"

s := dataframe.NewSeriesInt64("random", nil, 1, 2, 3, 4, 5, 6, 7, 8)
sf, _ := s.ToSeriesFloat64(ctx)
```

### Mean

```go
mean := stat.Mean(sf.Values, nil)
```

### Median

```go
import "github.com/montanaflynn/stats"
median, _ := stats.Median(sf.Values)
```

### Standard Deviation

```go
std := stat.StdDev(sf.Values, nil)
```

## Importing Data

The `imports` sub-package has support for importing csv, jsonl and directly from a SQL database. The `DictateDataType` option can be set to specify the true underlying data type. Alternatively, `InferDataTypes` option can be set.

### CSV

```go
csvStr := `
Country,Date,Age,Amount,Id
"United States",2012-02-01,50,112.1,01234
"United States",2012-02-01,32,321.31,54320
"United Kingdom",2012-02-01,17,18.2,12345
"United States",2012-02-01,32,321.31,54320
"United Kingdom",2012-02-01,NA,18.2,12345
"United States",2012-02-01,32,321.31,54320
"United States",2012-02-01,32,321.31,54320
Spain,2012-02-01,66,555.42,00241
`
df, err := imports.LoadFromCSV(ctx, strings.NewReader(csvStr))

OUTPUT:
+-----+----------------+------------+-------+---------+-------+
|     |    COUNTRY     |    DATE    |  AGE  | AMOUNT  |  ID   |
+-----+----------------+------------+-------+---------+-------+
| 0:  | United States  | 2012-02-01 |  50   |  112.1  | 1234  |
| 1:  | United States  | 2012-02-01 |  32   | 321.31  | 54320 |
| 2:  | United Kingdom | 2012-02-01 |  17   |  18.2   | 12345 |
| 3:  | United States  | 2012-02-01 |  32   | 321.31  | 54320 |
| 4:  | United Kingdom | 2015-05-07 |  NaN  |  18.2   | 12345 |
| 5:  | United States  | 2012-02-01 |  32   | 321.31  | 54320 |
| 6:  | United States  | 2012-02-01 |  32   | 321.31  | 54320 |
| 7:  |     Spain      | 2012-02-01 |  66   | 555.42  |  241  |
+-----+----------------+------------+-------+---------+-------+
| 8X5 |     STRING     |    TIME    | INT64 | FLOAT64 | INT64 |
+-----+----------------+------------+-------+---------+-------+
```

## Exporting Data

The `exports` sub-package has support for exporting to csv, jsonl, Excel and directly to a SQL database.


## Optimizations

* If you know the number of rows in advance, you can set the capacity of the underlying slice of a series using `SeriesInit{}`. This will preallocate memory and provide speed improvements. 

# Generic Series

Out of the box, there is support for `string`, `time.Time`, `float64` and `int64`. Automatic support exists for `float32` and all types of integers. There is a convenience function provided for dealing with `bool`. There is also support for `complex128` inside the `xseries` subpackage.

There may be times that you want to use your own custom data types. You can either implement your own `Series` type (more performant) or use the **Generic Series** (more convenient).

## civil.Date

```go

import (
  "time"
  "cloud.google.com/go/civil"
)

sg := dataframe.NewSeriesGeneric("date", civil.Date{}, nil, civil.Date{2018, time.May, 01}, civil.Date{2018, time.May, 02}, civil.Date{2018, time.May, 03})
s2 := dataframe.NewSeriesFloat64("sales", nil, 50.3, 23.4, 56.2)

df := dataframe.NewDataFrame(sg, s2)

OUTPUT:
+-----+------------+---------+
|     |    DATE    |  SALES  |
+-----+------------+---------+
| 0:  | 2018-05-01 |  50.3   |
| 1:  | 2018-05-02 |  23.4   |
| 2:  | 2018-05-03 |  56.2   |
+-----+------------+---------+
| 3X2 | CIVIL DATE | FLOAT64 |
+-----+------------+---------+

```

# Example

## Create some fake data

Let's create a list of 8 "fake" employees with a name, title and base hourly wage rate.

```go
import "golang.org/x/exp/rand"
import "rocketlaunchr/dataframe-go/utils/faker"

ctx := context.Background()
src := rand.NewSource(uint64(time.Now().UTC().UnixNano()))
df := faker.NewDataFrame(8, src, faker.S("name", 0, "Name"), faker.S("title", 0.5, "JobTitle"), faker.S("base rate", 0, "Number", 15, 50))
```

```go
+-----+----------------+----------------+-----------+
|     |      NAME      |     TITLE      | BASE RATE |
+-----+----------------+----------------+-----------+
| 0:  | Cordia Jacobi  |   Consultant   |    42     |
| 1:  | Nickolas Emard |      NaN       |    22     |
| 2:  | Hollis Dickens | Representative |    22     |
| 3:  | Stacy Dietrich |      NaN       |    43     |
| 4:  |  Aleen Legros  |    Officer     |    21     |
| 5:  |  Adelia Metz   |   Architect    |    18     |
| 6:  | Sunny Gerlach  |      NaN       |    28     |
| 7:  | Austin Hackett |      NaN       |    39     |
+-----+----------------+----------------+-----------+
| 8X3 |     STRING     |     STRING     |   INT64   |
+-----+----------------+----------------+-----------+
```

## Apply Function

Let's give a promotion to everyone by doubling their salary.

```go
s := df.Series[2]

applyFn := dataframe.ApplySeriesFn(func(val interface{}, row, nRows int) interface{} {
	return 2 * val.(int64)
})

dataframe.Apply(ctx, s, applyFn, dataframe.FilterOptions{InPlace: true})
```

```go
+-----+----------------+----------------+-----------+
|     |      NAME      |     TITLE      | BASE RATE |
+-----+----------------+----------------+-----------+
| 0:  | Cordia Jacobi  |   Consultant   |    84     |
| 1:  | Nickolas Emard |      NaN       |    44     |
| 2:  | Hollis Dickens | Representative |    44     |
| 3:  | Stacy Dietrich |      NaN       |    86     |
| 4:  |  Aleen Legros  |    Officer     |    42     |
| 5:  |  Adelia Metz   |   Architect    |    36     |
| 6:  | Sunny Gerlach  |      NaN       |    56     |
| 7:  | Austin Hackett |      NaN       |    78     |
+-----+----------------+----------------+-----------+
| 8X3 |     STRING     |     STRING     |   INT64   |
+-----+----------------+----------------+-----------+
```


## Create a Time series

Let's inform all employees separately on sequential days.

```go
import "rocketlaunchr/dataframe-go/utils/utime"

mts, _ := utime.NewSeriesTime(ctx, "meeting time", "1D", time.Now().UTC(), false, utime.NewSeriesTimeOptions{Size: &[]int{8}[0]})
df.AddSeries(mts, nil)
```

```go
+-----+----------------+----------------+-----------+--------------------------------+
|     |      NAME      |     TITLE      | BASE RATE |          MEETING TIME          |
+-----+----------------+----------------+-----------+--------------------------------+
| 0:  | Cordia Jacobi  |   Consultant   |    84     |   2020-02-02 23:13:53.015324   |
|     |                |                |           |           +0000 UTC            |
| 1:  | Nickolas Emard |      NaN       |    44     |   2020-02-03 23:13:53.015324   |
|     |                |                |           |           +0000 UTC            |
| 2:  | Hollis Dickens | Representative |    44     |   2020-02-04 23:13:53.015324   |
|     |                |                |           |           +0000 UTC            |
| 3:  | Stacy Dietrich |      NaN       |    86     |   2020-02-05 23:13:53.015324   |
|     |                |                |           |           +0000 UTC            |
| 4:  |  Aleen Legros  |    Officer     |    42     |   2020-02-06 23:13:53.015324   |
|     |                |                |           |           +0000 UTC            |
| 5:  |  Adelia Metz   |   Architect    |    36     |   2020-02-07 23:13:53.015324   |
|     |                |                |           |           +0000 UTC            |
| 6:  | Sunny Gerlach  |      NaN       |    56     |   2020-02-08 23:13:53.015324   |
|     |                |                |           |           +0000 UTC            |
| 7:  | Austin Hackett |      NaN       |    78     |   2020-02-09 23:13:53.015324   |
|     |                |                |           |           +0000 UTC            |
+-----+----------------+----------------+-----------+--------------------------------+
| 8X4 |     STRING     |     STRING     |   INT64   |              TIME              |
+-----+----------------+----------------+-----------+--------------------------------+
```

## Filtering

Let's filter out our senior employees (they have titles) for no reason.

```go
filterFn := dataframe.FilterDataFrameFn(func(vals map[interface{}]interface{}, row, nRows int) (dataframe.FilterAction, error) {
	if vals["title"] == nil {
		return dataframe.DROP, nil
	}
	return dataframe.KEEP, nil
})

seniors, _ := dataframe.Filter(ctx, df, filterFn)
```

```go
+-----+----------------+----------------+-----------+--------------------------------+
|     |      NAME      |     TITLE      | BASE RATE |          MEETING TIME          |
+-----+----------------+----------------+-----------+--------------------------------+
| 0:  | Cordia Jacobi  |   Consultant   |    84     |   2020-02-02 23:13:53.015324   |
|     |                |                |           |           +0000 UTC            |
| 1:  | Hollis Dickens | Representative |    44     |   2020-02-04 23:13:53.015324   |
|     |                |                |           |           +0000 UTC            |
| 2:  |  Aleen Legros  |    Officer     |    42     |   2020-02-06 23:13:53.015324   |
|     |                |                |           |           +0000 UTC            |
| 3:  |  Adelia Metz   |   Architect    |    36     |   2020-02-07 23:13:53.015324   |
|     |                |                |           |           +0000 UTC            |
+-----+----------------+----------------+-----------+--------------------------------+
| 4X4 |     STRING     |     STRING     |   INT64   |              TIME              |
+-----+----------------+----------------+-----------+--------------------------------+
```


## Other useful packages

- [dbq](https://github.com/rocketlaunchr/dbq) - Zero boilerplate database operations for Go
- [electron-alert](https://github.com/rocketlaunchr/electron-alert) - SweetAlert2 for Electron Applications
- [igo](https://github.com/rocketlaunchr/igo) - A Go transpiler with cool new syntax such as fordefer (defer for for-loops)
- [mysql-go](https://github.com/rocketlaunchr/mysql-go) - Properly cancel slow MySQL queries
- [react](https://github.com/rocketlaunchr/react) - Build front end applications using Go
- [remember-go](https://github.com/rocketlaunchr/remember-go) - Cache slow database queries

#

### Legal Information

The license is a modified MIT license. Refer to `LICENSE` file for more details.

**© 2018-20 PJ Engineering and Business Solutions Pty. Ltd.**
