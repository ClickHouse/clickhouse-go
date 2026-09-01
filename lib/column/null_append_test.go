package column

import (
	"database/sql"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
)

func TestColumnarNullAppend(t *testing.T) {
	date := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	datetime := time.Date(2024, 1, 2, 3, 4, 5, 123000000, time.UTC)

	int16Valid := sql.NullInt16{Int16: 16, Valid: true}
	int16Invalid := sql.NullInt16{Int16: 99, Valid: false}
	int32Valid := sql.NullInt32{Int32: 32, Valid: true}
	int32Invalid := sql.NullInt32{Int32: 99, Valid: false}
	int64Valid := sql.NullInt64{Int64: 64, Valid: true}
	int64Invalid := sql.NullInt64{Int64: 99, Valid: false}
	float64Valid := sql.NullFloat64{Float64: 64.5, Valid: true}
	float64Invalid := sql.NullFloat64{Float64: 99.5, Valid: false}
	timeValid := sql.NullTime{Time: datetime, Valid: true}
	timeInvalid := sql.NullTime{Time: date, Valid: false}
	stringValid := sql.NullString{String: "valid", Valid: true}
	stringInvalid := sql.NullString{String: "invalid", Valid: false}

	tests := []struct {
		name         string
		newColumn    func() Interface
		values       any
		pointers     any
		valueNulls   []uint8
		pointerNulls []uint8
		valueRows    int
		pointerRows  int
	}{
		{
			name:       "Float64",
			newColumn:  func() Interface { return &Float64{name: "test"} },
			values:     []sql.NullFloat64{float64Valid, float64Invalid},
			pointers:   []*sql.NullFloat64{&float64Valid, &float64Invalid, nil},
			valueNulls: []uint8{0, 1}, pointerNulls: []uint8{0, 1, 1},
			valueRows: 2, pointerRows: 3,
		},
		{
			name:       "Int16",
			newColumn:  func() Interface { return &Int16{name: "test"} },
			values:     []sql.NullInt16{int16Valid, int16Invalid},
			pointers:   []*sql.NullInt16{&int16Valid, &int16Invalid, nil},
			valueNulls: []uint8{0, 1}, pointerNulls: []uint8{0, 1, 1},
			valueRows: 2, pointerRows: 3,
		},
		{
			name:       "Int32",
			newColumn:  func() Interface { return &Int32{name: "test"} },
			values:     []sql.NullInt32{int32Valid, int32Invalid},
			pointers:   []*sql.NullInt32{&int32Valid, &int32Invalid, nil},
			valueNulls: []uint8{0, 1}, pointerNulls: []uint8{0, 1, 1},
			valueRows: 2, pointerRows: 3,
		},
		{
			name:       "Int64",
			newColumn:  func() Interface { return &Int64{name: "test"} },
			values:     []sql.NullInt64{int64Valid, int64Invalid},
			pointers:   []*sql.NullInt64{&int64Valid, &int64Invalid, nil},
			valueNulls: []uint8{0, 1}, pointerNulls: []uint8{0, 1, 1},
			valueRows: 2, pointerRows: 3,
		},
		{
			name:       "Date",
			newColumn:  func() Interface { return &Date{name: "test"} },
			values:     []sql.NullTime{timeValid, timeInvalid},
			pointers:   []*sql.NullTime{&timeValid, &timeInvalid, nil},
			valueNulls: []uint8{0, 1}, pointerNulls: []uint8{0, 1, 1},
			valueRows: 2, pointerRows: 3,
		},
		{
			name:       "Date32",
			newColumn:  func() Interface { return &Date32{name: "test"} },
			values:     []sql.NullTime{timeValid, timeInvalid},
			pointers:   []*sql.NullTime{&timeValid, &timeInvalid, nil},
			valueNulls: []uint8{0, 1}, pointerNulls: []uint8{0, 1, 1},
			valueRows: 2, pointerRows: 3,
		},
		{
			name: "DateTime",
			newColumn: func() Interface {
				return &DateTime{name: "test"}
			},
			values:     []sql.NullTime{timeValid, timeInvalid},
			pointers:   []*sql.NullTime{&timeValid, &timeInvalid, nil},
			valueNulls: []uint8{0, 1}, pointerNulls: []uint8{0, 1, 1},
			valueRows: 2, pointerRows: 3,
		},
		{
			name: "DateTime64",
			newColumn: func() Interface {
				return &DateTime64{
					name: "test",
					col:  proto.ColDateTime64{Precision: 3, PrecisionSet: true},
				}
			},
			values:     []sql.NullTime{timeValid, timeInvalid},
			pointers:   []*sql.NullTime{&timeValid, &timeInvalid, nil},
			valueNulls: []uint8{0, 1}, pointerNulls: []uint8{0, 1, 1},
			valueRows: 2, pointerRows: 3,
		},
		{
			name:       "String",
			newColumn:  func() Interface { return &String{name: "test"} },
			values:     []sql.NullString{stringValid, stringInvalid},
			pointers:   []*sql.NullString{&stringValid, &stringInvalid, nil},
			valueNulls: []uint8{0, 1}, pointerNulls: []uint8{0, 1, 1},
			valueRows: 2, pointerRows: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("values", func(t *testing.T) {
				col := tt.newColumn()
				nulls, err := col.Append(tt.values)
				require.NoError(t, err)
				require.Equal(t, tt.valueNulls, nulls)
				require.Equal(t, tt.valueRows, col.Rows())
			})

			t.Run("pointers", func(t *testing.T) {
				col := tt.newColumn()
				nulls, err := col.Append(tt.pointers)
				require.NoError(t, err)
				require.Equal(t, tt.pointerNulls, nulls)
				require.Equal(t, tt.pointerRows, col.Rows())
			})
		})
	}
}
