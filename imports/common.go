// Copyright 2018-21 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

// Package imports provides functionality to read data contained in another format to populate a DataFrame.
// It provides inverse functionality to the exports package.
package imports

// GenericDataConverter is used to convert input data into a generic data type.
// This is required when importing data for a Generic Series ("SeriesGeneric").
type GenericDataConverter func(in interface{}) (interface{}, error)

// Converter is used to convert input data into a generic data type.
// This is required when importing data for a Generic Series ("dataframe.SeriesGeneric").
// As a special case, if ConcreteType is time.Time, then a SeriesTime is used.
//
// Example:
//
//  opts := imports.CSVLoadOptions{
//     DictateDataType: map[string]interface{}{
//        "Date": imports.Converter{
//           ConcreteType: time.Time{},
//           ConverterFunc: func(in interface{}) (interface{}, error) {
//              return time.Parse("2006-01-02", in.(string))
//           },
//        },
//     },
//  }
//
type Converter struct {
	ConcreteType  interface{}
	ConverterFunc GenericDataConverter
}

// parseObject converts maps within maps and moves them to the root level with
// dots.
// eg. {"A":123, "B":{"C": "D"}} => {"A":123, "B.C":"D"}
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
