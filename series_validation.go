package dataframe

import (
	"fmt"
	"reflect"
)

// checkConcreteType checks if ct is the zero-value of a concrete data type
func checkConcreteType(ct interface{}) error {

	if ct == nil {
		return fmt.Errorf("%v is not a valid concrete type", ct)
	}

	// Reject unacceptable types
	s := reflect.ValueOf(ct)
	switch s.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128,
		reflect.String, reflect.Struct:

		// Make sure concrete type is zero value
		if !reflect.DeepEqual(ct, reflect.Zero(reflect.TypeOf(ct)).Interface()) {
			return fmt.Errorf("%v is not the zero value", ct)
		}
	default:
		return fmt.Errorf("%T is not a valid concrete type", ct)
	}

	return nil
}

// checkValue checks if an input value is valid.
// In order to be valid, it must be either nil or
// a non-pointer of the same type as the series's
// concrete type.
func (s *SeriesGeneric) checkValue(v interface{}) error {

	if v == nil {
		return nil
	}

	// Check if v is a pointer
	if reflect.TypeOf(v) != reflect.TypeOf(s.concreteType) {
		return fmt.Errorf("%v: value is invalid type", v)
	}

	return nil
}

// func MarshalJSON(_s interface{}, hidden map[string]struct{}) ([]byte, error) {
// 	out := map[string]interface{}{}

// 	// Encode nil immediately
// 	if _s == nil {
// 		return []byte("null"), nil
// 	}

// 	s := reflect.ValueOf(_s)

// 	// Check if s is a pointer
// 	if s.Kind() == reflect.Ptr {
// 		s = reflect.Indirect(s)
// 	}
// 	typeOfT := s.Type()

// 	for i := 0; i < s.NumField(); i++ {
// 		f := typeOfT.Field(i)

// 		if f.PkgPath != "" {
// 			//not exported
// 			continue
// 		}

// 		fieldName := typeOfT.Field(i).Name
// 		fieldTag := f.Tag.Get("json")
// 		fieldVal := s.Field(i).Interface()

// 		// Automatically hide Hidden struct
// 		_, ok := fieldVal.(Hidden)
// 		if ok {
// 			continue
// 		}

// 		//Check if json parser would ordinarily hide the value anyway
// 		if fieldTag == "-" || (strings.HasSuffix(fieldTag, ",omitempty") && reflect.DeepEqual(fieldVal, reflect.Zero(reflect.TypeOf(fieldVal)).Interface())) {
// 			continue
// 		}

// 		if _, exists := hidden[fieldName]; exists {
// 			continue
// 		}

// 		if fieldTag == "" {
// 			out[fieldName] = fieldVal
// 		} else {
// 			out[strings.TrimSuffix(fieldTag, ",omitempty")] = fieldVal
// 		}
// 	}

// 	return json.Marshal(out)
// }
