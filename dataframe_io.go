package dataframe

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"log"
)

func FromCSV(r io.Reader) (*DataFrame, error) {
	var df *DataFrame
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
			df1 := NewDataFrame(series...)
			df = df1
		} else {
			var values []interface{}
			for _, value := range line {
				values = append(values, value)
			}
			df.Append(values...)
		}

	}
	if len(series) == 0 {
		return nil, errors.New("csv data contains no rows")
	}
	return df, nil
}

func (df *DataFrame) ToCSV(w io.Writer) {
	df.Lock()
	defer df.Unlock()
	writer := csv.NewWriter(w)
	defer writer.Flush()

	numOfSeries := len(df.Series)
	numOfRows := df.Series[0].NRows()
	writer.Write(df.Names())
	for j := 0; j < numOfRows; j++ {
		var res []string
		for i := 0; i < numOfSeries; i++ {
			res = append(res, df.Series[i].ValueString(j))
		}
		writer.Write(res)
	}
}

func (df *DataFrame) ToJSON(w io.Writer){
	var out [][]string
	numOfSeries := len(df.Series)
	numOfRows := df.Series[0].NRows()
	out = append(out,df.Names())
	for j := 0; j < numOfRows; j++ {
		var res []string
		for i := 0; i < numOfSeries; i++ {
			res = append(res,df.Series[i].ValueString(j))
		}
		out = append(out,res)
	}
	bytes,_ := json.Marshal(out)

	w.Write(bytes)
}
