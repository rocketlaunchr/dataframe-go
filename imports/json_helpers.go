// Copyright 2018-21 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/goccy/go-json"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

type jsonRow func() (*int, interface{}, error) // row, data

type jsonFormat int

const (
	jsonUnknown jsonFormat = iota
	jsonArray
	jsonlObj   // lines of objects
	jsonlArray // lines of arrays (with first line containing column names)
)

func detectJSONDataFormat(r io.ReadSeeker) (jsonFormat, error) {
	defer r.Seek(0, io.SeekStart)

	dec := json.NewDecoder(r)

	t, err := dec.Token()
	if err != nil {
		return jsonUnknown, fmt.Errorf("invalid input: %w", err)
	}

	switch v := t.(type) {
	default:
		return jsonUnknown, fmt.Errorf("invalid input: %v", v)
	case json.Delim:
		if v == '[' {
			// Check next token
			t, err := dec.Token()
			if err != nil {
				return jsonUnknown, fmt.Errorf("invalid input: %w", err)
			}

			x, ok := t.(json.Delim)
			if ok && x == '{' {
				// actual json array
				return jsonArray, nil
			} else {
				// lines of array
				return jsonlArray, nil
			}
		} else if v == '{' {
			// lines of objects
			return jsonlObj, nil
		} else {
			return jsonUnknown, fmt.Errorf("invalid input: %v", v)
		}
	}
	panic("should not reach here")
}

func readJSON(r io.ReadSeeker) (jsonRow, jsonFormat, error) {
	fmt, err := detectJSONDataFormat(r)
	if err != nil {
		return nil, fmt, err
	}

	if fmt == jsonArray {
		return processArray(r), fmt, nil
	} else {
		return processLines(r), fmt, nil
	}
}

func processArray(r io.ReadSeeker) jsonRow {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	dec.Token()
	count := -1
	return func() (*int, interface{}, error) {
		if dec.More() {
			var m map[string]interface{} // array containing objects
			err := dec.Decode(&m)
			if err != nil {
				return nil, nil, err
			}
			count++
			return &count, parseObject(m, ""), nil
		} else {
			// No more rows
			return nil, nil, nil
		}
	}
}

func processLines(r io.ReadSeeker) jsonRow {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	count := -1

	var jsonlArrayCols []string

	return func() (*int, interface{}, error) {
		var m interface{} // lines of objects OR lines of arrays
		if err := dec.Decode(&m); err == io.EOF {
			// No more rows
			return nil, nil, nil
		} else if err != nil {
			if jsonlArrayCols == nil {
				// lines of objects (NB: Doesn't seem to reach here)
				return nil, nil, fmt.Errorf("error in row: %d (line: %d): %w", count+1, count+2, err)
			} else {
				// lines of arrays
				return nil, nil, fmt.Errorf("error in row: %d (line: %d): %w", count+1, count+3, err)
			}
		}

		// We have a row of data
		if nr, ok := m.([]interface{}); ok {
			// We have encountered jsonl in ARRAY format.

			if count == -1 { // first row of jsonl in ARRAY format
				// m contains an array of series names
				for _, v := range nr {
					sname, valid := v.(string)
					if !valid {
						return nil, nil, errors.New("first row must contain an array of series names")
					}
					jsonlArrayCols = append(jsonlArrayCols, sname)
				}

				var m2 []interface{} // m2 contains second row in data (but our "first" row)
				err := dec.Decode(&m2)
				if err != nil {
					if err == io.EOF {
						return nil, nil, dataframe.ErrNoRows
					} else {
						return nil, nil, err
					}
				}
				nr = m2
			}

			rowVals := map[string]interface{}{}

			if len(nr) != len(jsonlArrayCols) {
				return nil, nil, fmt.Errorf("too many or too few values found in row: %d (line: %d)", count+1, count+3)
			}

			for i, v := range nr {
				rowVals[jsonlArrayCols[i]] = v
			}

			if count == -1 {
				m = []interface{}{jsonlArrayCols, rowVals}
			} else {
				m = rowVals
			}
		} else {
			// We have encountered jsonl in OBJECT format.
			m = parseObject(m.(map[string]interface{}), "")
		}

		count++
		return &count, m, nil
	}
}

func dictateForce(row int, name string, typ interface{}, val interface{}) (insertVal interface{}, _ error) {
	switch T := typ.(type) {
	case nil:
		panic("invalid dictated datatype for " + name)
	case float64:
		// Force v to float64
		switch v := val.(type) {
		case nil:
			insertVal = nil
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("can't force string: %s to float64. row: %d field: %s", v, row, name)
			}
			insertVal = f
		case json.Number:
			f, err := v.Float64()
			if err != nil {
				return nil, fmt.Errorf("can't force number to float64. row: %d field: %s", row, name)
			}
			insertVal = f
		case bool:
			if v == true {
				insertVal = 1.0
			} else {
				insertVal = 0.0
			}
		}
	case bool:
		// Force v to int64 (bools are treated as int64)
		switch v := val.(type) {
		case nil:
			insertVal = nil
		case string:
			if v == "TRUE" || v == "true" || v == "True" || v == "1" {
				insertVal = int64(1)
			} else if v == "FALSE" || v == "false" || v == "False" || v == "0" {
				insertVal = int64(0)
			} else {
				return nil, fmt.Errorf("can't force string: %s to bool. row: %d field: %s", v, row, name)
			}
		case json.Number:
			// Check if float64
			f, err := v.Float64()
			if err != nil {
				return nil, fmt.Errorf("can't force number to bool. row: %d field: %s", row, name)
			}

			if f == 1 {
				insertVal = int64(1)
			} else if f == 0 {
				insertVal = int64(0)
			} else {
				return nil, fmt.Errorf("can't force number to bool. row: %d field: %s", row, name)
			}
		case bool:
			if v == true {
				insertVal = int64(1)
			} else {
				insertVal = int64(0)
			}
		}
	case int:
		// Force v to int64
		switch v := val.(type) {
		case nil:
			insertVal = nil
		case string:
			i, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("can't force string: %s to int64. row: %d field: %s", v, row, name)
			}
			insertVal = i
		case json.Number:
			i, err := v.Int64()
			if err != nil {
				return nil, fmt.Errorf("can't force number to int64. row: %d field: %s", row, name)
			}
			insertVal = i
		case bool:
			if v == true {
				insertVal = int64(1)
			} else {
				insertVal = int64(0)
			}
		}
	case int64:
		// Force v to int64
		switch v := val.(type) {
		case nil:
			insertVal = nil
		case string:
			i, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("can't force string: %s to int64. row: %d field: %s", v, row, name)
			}
			insertVal = i
		case json.Number:
			i, err := v.Int64()
			if err != nil {
				return nil, fmt.Errorf("can't force number to int64. row: %d field: %s", row, name)
			}
			insertVal = i
		case bool:
			if v == true {
				insertVal = int64(1)
			} else {
				insertVal = int64(0)
			}
		}
	case string:
		// Force v to string
		switch v := val.(type) {
		case nil:
			insertVal = nil
		case string:
			insertVal = v
		case json.Number:
			insertVal = v.String()
		case bool:
			if v == true {
				insertVal = "true"
			} else {
				insertVal = "false"
			}
		}
	case time.Time:
		// Force v to time
		switch v := val.(type) {
		case nil:
			insertVal = nil
		case string:
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return nil, fmt.Errorf("can't force string: %s to time.Time (%s). row: %d field: %s", v, time.RFC3339, row, name)
			}
			insertVal = t
		case json.Number:
			// Assume unix timestamp
			sec, err := v.Int64()
			if err != nil {
				return nil, fmt.Errorf("can't force number to int64 (unix timestamp). row: %d field: %s", row, name)
			}
			insertVal = time.Unix(sec, 0)
		default:
			return nil, fmt.Errorf("can't force %T to time.Time. row: %d field: %s", v, row, name)
		}
	case dataframe.NewSerieser:
		// Force v to string
		switch v := val.(type) {
		case nil:
			insertVal = nil
		case string:
			insertVal = v
		case json.Number:
			insertVal = v.String()
		case bool:
			if v == true {
				insertVal = "true"
			} else {
				insertVal = "false"
			}
		}
	case Converter:
		// Force v to generic
		cv, err := T.ConverterFunc(val)
		if err != nil {
			return nil, fmt.Errorf("can't force %T to generic data type. row: %d field: %s", val, row, name)
		}
		insertVal = cv
	default:
		panic("TODO: not implemented")
	}
	return
}
