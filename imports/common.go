// Copyright 2018 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// GenericDataConverter is used to convert input data into a generic data type.
// This is required when importing data for a Generic Series ("NewSeries").
type GenericDataConverter func(in interface{}) (interface{}, error)

// Converter is used to convert input data into a generic data type.
// This is required when importing data for a Generic Series ("NewSeries").
type Converter struct {
	ConcreteType  interface{}
	ConverterFunc GenericDataConverter
}

func parseObject(v map[string]interface{}, prefix string) map[string]interface{} {

	out := map[string]interface{}{}

	for k, t := range v {
		var key string
		if prefix == "" {
			key = k
		} else {
			key = prefix + "." + k
		}

		switch v := t.(type) {
		case map[string]interface{}:
			for k, t := range parseObject(v, key) {
				out[k] = t
			}
		default:
			out[key] = t
		}
	}

	return out
}

func dictateForce(row int, insertVals map[string]interface{}, name string, typ interface{}, val interface{}) error {
	switch T := typ.(type) {
	case float64:
		// Force v to float64
		switch v := val.(type) {
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return fmt.Errorf("can't force string to float64. row: %d field: %s", row-1, name)
			}
			insertVals[name] = f
		case json.Number:
			f, err := v.Float64()
			if err != nil {
				return fmt.Errorf("can't force number to float64. row: %d field: %s", row-1, name)
			}
			insertVals[name] = f
		case bool:
			if v == true {
				insertVals[name] = 1.0
			} else {
				insertVals[name] = 0.0
			}
		}
	case bool:
		// Force v to int64 (bools are treated as int64)
		switch v := val.(type) {
		case string:
			if v == "TRUE" || v == "true" || v == "1" {
				insertVals[name] = int64(1)
			} else if v == "FALSE" || v == "false" || v == "0" {
				insertVals[name] = int64(0)
			} else {
				return fmt.Errorf("can't force string to bool. row: %d field: %s", row-1, name)
			}
		case json.Number:
			// Check if float64
			f, err := v.Float64()
			if err != nil {
				return fmt.Errorf("can't force number to bool. row: %d field: %s", row-1, name)
			}

			if f == 1 {
				insertVals[name] = int64(1)
			} else if f == 0 {
				insertVals[name] = int64(0)
			} else {
				return fmt.Errorf("can't force number to bool. row: %d field: %s", row-1, name)
			}
		case bool:
			if v == true {
				insertVals[name] = int64(1)
			} else {
				insertVals[name] = int64(0)
			}
		}
	case int64:
		// Force v to int64
		switch v := val.(type) {
		case string:
			i, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return fmt.Errorf("can't force string to int64. row: %d field: %s", row-1, name)
			}
			insertVals[name] = i
		case json.Number:
			i, err := v.Int64()
			if err != nil {
				return fmt.Errorf("can't force number to int64. row: %d field: %s", row-1, name)
			}
			insertVals[name] = i
		case bool:
			if v == true {
				insertVals[name] = int64(1)
			} else {
				insertVals[name] = int64(0)
			}
		}
	case string:
		// Force v to string
		switch v := val.(type) {
		case string:
			insertVals[name] = v
		case json.Number:
			insertVals[name] = v.String()
		case bool:
			if v == true {
				insertVals[name] = "true"
			} else {
				insertVals[name] = "false"
			}
		}
	case time.Time:
		// Force v to time
		switch v := val.(type) {
		case string:
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return fmt.Errorf("can't force string to time.Time (%s). row: %d field: %s", time.RFC3339, row-1, name)
			}
			insertVals[name] = t
		case json.Number:
			// Assume unix timestamp
			sec, err := v.Int64()
			if err != nil {
				return fmt.Errorf("can't force number to int64 (unix timestamp). row: %d field: %s", row-1, name)
			}
			insertVals[name] = time.Unix(sec, 0)
		case nil:
			// Do nothing
		default:
			return fmt.Errorf("can't force %T to time.Time. row: %d field: %s", v, row-1, name)
		}
	case Converter:
		// Force v to generic
		cv, err := T.ConverterFunc(val)
		if err != nil {
			return fmt.Errorf("can't force %T to generic data type. row: %d field: %s", val, row-1, name)
		}
		insertVals[name] = cv
	default:
		// Force v to generic
		panic("TODO: Not implemented")
	}

	return nil
}
