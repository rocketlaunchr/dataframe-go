package exports

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/rocketlaunchr/dataframe-go/imports"
	"github.com/xitongsys/parquet-go/parquet"
)

func TestParquetExport(t *testing.T) {
	jsonStr := `{"name": "vikash", "age": 27, "price": 27.44, "zzz": 27.0}{"name": "ABC", "age": 99}{"name": "Satyam", "age": 14}
	 {"price": 456, "age":8}
	`

	df, err := imports.LoadFromJSON(context.Background(), strings.NewReader(jsonStr),
		imports.JSONLoadOptions{LargeDataSet: true,
			DictateDataType: map[string]interface{}{
				"name":  string(""),
				"age":   int64(0),
				"price": float64(0),
				"zzz":   float64(0)},
			ErrorOnUnknownFields: false})
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	fmt.Print(df)

	// md := []string{
	// 	"name=age, type=INT32",
	// 	"name=name, type=UTF8, encoding=PLAIN_DICTIONARY",
	// 	"name=price, type=DOUBLE",
	// 	"name=zzz, type=DOUBLE",
	// }

	file, err := os.OpenFile("output.parquet", os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	if err := ExportToParquet(
		context.Background(),
		file, df,
		ParquetExportOptions{
			// NullString:      &[]string{"NaN"}[0],
			// RowGroupSize:    &[]int64{128 * 1024 * 1024}[0], //128M
			CompressionType: parquet.CompressionCodec_SNAPPY,
		},
	); err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
	fmt.Println("finish parquet export to output.parquet file")
}

func TestExcelExport(t *testing.T) {
	jsonStr := `{"name": "vikash", "age": 27, "price": 27.44, "zzz": 27.0}{"name": "ABC", "age": 99}{"name": "Satyam", "age": 14}
	 {"name": "Desmond", "price": 456, "age":8}
	`
	df, err := imports.LoadFromJSON(context.Background(), strings.NewReader(jsonStr),
		imports.JSONLoadOptions{LargeDataSet: true,
			DictateDataType: map[string]interface{}{
				"name":  string(""),
				"age":   int64(0),
				"price": float64(0)},
			ErrorOnUnknownFields: false})
	if err != nil {
		fmt.Println(err)
	}

	fmt.Print(df)

	file, err := os.OpenFile("excelOutput.xlsx", os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}

	defer file.Close()

	err = ExportToExcel(context.Background(),
		file, df,
		ExcelExportOptions{NullString: &[]string{"empty"}[0]},
	)

	if err != nil {
		t.Errorf("error encountered: %s\n", err)
	}
}
