package stat

import (
	"fmt"
	"github.com/rocketlaunchr/dataframe-go"
	"testing"
)

func TestSeriesFloat64_Abs(t *testing.T) {

	s1 := SeriesFloat64{*dataframe.NewSeriesFloat64("num",nil,-1.1,1.1,nil)}
	s1.Abs()

	expectedValues := []interface{}{1.1,1.1,"NaN"}
	n := s1.NRows()
	for i :=0 ; i < n; i ++ {
		exVal := s1.ValueString(i)
		exp := expectedValues[i]
		if exVal != fmt.Sprintf("%v", exp) {
			t.Errorf("wrong val: expected: %v actual: %v", exp, exVal)
		}
	}

}

func TestSeriesInt64_Abs(t *testing.T) {
	s1 := SeriesInt64{*dataframe.NewSeriesInt64("num",nil,-1,1,nil)}
	s1.Abs()

	expectedValues := []interface{}{1,1,"NaN"}
	n := s1.NRows()
	for i :=0 ; i < n; i ++ {
		exVal := s1.ValueString(i)
		exp := expectedValues[i]
		if exVal != fmt.Sprintf("%v", exp) {
			t.Errorf("wrong val: expected: %v actual: %v", exp, exVal)
		}
	}
}
