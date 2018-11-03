package stat

import (
	"fmt"
	"github.com/rocketlaunchr/dataframe-go"
	"testing"
)

func TestSeriesFloat64_Abs(t *testing.T) {
	s1 := StFloat64{dataframe.NewSeriesFloat64("num",nil,-1.1,1.1)}
	s1.Abs()
	fmt.Println(s1.String())
}
