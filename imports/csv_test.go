// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package imports

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	dataframe "github.com/rocketlaunchr/dataframe-go"
	"github.com/stretchr/testify/assert"
)

func assertEqualDS(t *testing.T, want *dataframe.DataFrame, got *dataframe.DataFrame) {
	assert.Equal(t, len(want.Series), len(got.Series))
	assert.Equal(t, want.NRows(), got.NRows())
	for i := 0; i < len(want.Series); i++ {
		assert.Equal(t, want.Series[i].NRows(), got.Series[i].NRows())

		for j := 0; j < want.NRows(); j++ {
			assert.Equal(t, want.Series[i].Value(j), got.Series[i].Value(j))
		}
	}
}

func TestLoadFromCSV(t *testing.T) {
	type args struct {
		file    string
		options CSVLoadOptions
	}
	tests := []struct {
		name    string
		args    args
		want    *dataframe.DataFrame
		wantErr bool
	}{
		{
			name: "Should parse valid CSV",
			args: args{
				file: "valid.csv",
			},
			want: dataframe.NewDataFrame(
				dataframe.NewSeriesString("time", nil, "1", "2", "3", "4", "5"),
				dataframe.NewSeriesString("text", nil, "col2-1", "col2-2", "col2-3", "col2-4", "col2-5"),
				dataframe.NewSeriesString("decimal", nil, "0.1", "0.2", "0.3", "0.4", "0.5"),
				dataframe.NewSeriesString("boolean", nil, "false", "true", "false", "true", "false"),
			),
		},
		{
			name: "Should parse valid CSV, without headers",
			args: args{
				file: "valid_without_headers.csv",
				options: CSVLoadOptions{
					ColumnNames: []string{"time-nh", "text-nh", "decimal-nh", "boolean-nh"},
				},
			},
			want: dataframe.NewDataFrame(
				dataframe.NewSeriesString("time-nh", nil, "11", "12", "13", "14", "15"),
				dataframe.NewSeriesString("text-nh", nil, "col2-11", "col2-12", "col2-13", "col2-14", "col2-15"),
				dataframe.NewSeriesString("decimal-nh", nil, "1.1", "1.2", "1.3", "1.4", "1.5"),
				dataframe.NewSeriesString("boolean-nh", nil, "false", "true", "false", "true", "false"),
			),
		},
		{
			name: "Should parse valid CSV, with specified type",
			args: args{
				file: "valid.csv",
				options: CSVLoadOptions{
					DictateDataType: map[string]interface{}{
						"time":    time.Time{},
						"text":    "",
						"decimal": float64(0),
						"boolean": false,
					},
				},
			},
			want: dataframe.NewDataFrame(
				dataframe.NewSeriesTime("time", nil, time.Unix(1, 0), time.Unix(2, 0), time.Unix(3, 0), time.Unix(4, 0), time.Unix(5, 0)),
				dataframe.NewSeriesString("text", nil, "col2-1", "col2-2", "col2-3", "col2-4", "col2-5"),
				dataframe.NewSeriesFloat64("decimal", nil, 0.1, 0.2, 0.3, 0.4, 0.5),
				dataframe.NewSeriesInt64("boolean", nil, 0, 1, 0, 1, 0),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := os.Open(filepath.Join("testdata", tt.args.file))
			assert.Nil(t, err)

			got, err := LoadFromCSV(context.TODO(), file, tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadFromCSV() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assertEqualDS(t, tt.want, got)
		})
	}
}
