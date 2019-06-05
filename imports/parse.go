// Copyright 2018 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

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
	switch typ.(type) {
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
	default:
		// Force v to generic
		panic("TODO: Not implemented")
	}

	return nil
}
