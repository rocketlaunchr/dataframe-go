package dataframe

import (
	"encoding/csv"
	"github.com/pkg/errors"
	"io"
	"log"
)

func (df *DataFrame) FromCSV(r io.Reader) error {
	var series []Series
	reader := csv.NewReader(r)
	reader.ReuseRecord = true
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		if len(series) == 0 {
			for _, value := range line {
				series = append(series, NewSeriesString(value, nil))
			}
			df = NewDataFrame(series...)
		} else {
			var values []interface{}
			for _, value := range line {
				values = append(values, value)
			}
			df.Append(values...)
		}

	}
	if len(series) == 0 {
		return errors.New("csv data contains no rows")
	}
	return nil
}

func (df *DataFrame) ToCSV(w io.Writer) {
	writer := csv.NewWriter(w)
	defer writer.Flush()
	
	numOfSeries := len(df.Series)
	numOfRows := df.Series[0].NRows()
	writer.Write(df.Names())
	for j := 0; j < numOfRows; j++ {
		var res []string
		for i := 0; i < numOfSeries; i++ {
			res = append(res,df.Series[i].ValueString(j))
		}
		writer.Write(res)
	}
}
