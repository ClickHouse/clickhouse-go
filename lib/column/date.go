package column

import (
	"fmt"
	"reflect"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

const secInDay = 24 * 60 * 60

type Date struct {
	values Int16
}

func (dt *Date) Type() Type {
	return "Date"
}

func (col *Date) ScanType() reflect.Type {
	return scanTypeTime
}

func (dt *Date) Rows() int {
	return len(dt.values)
}

func (dt *Date) Row(i int, ptr bool) interface{} {
	value := dt.row(i)
	if ptr {
		return &value
	}
	return value
}

func (dt *Date) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *time.Time:
		*d = dt.row(row)
	case **time.Time:
		*d = new(time.Time)
		**d = dt.row(row)
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Date",
		}
	}
	return nil
}

func (dt *Date) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []time.Time:
		in := make([]int16, 0, len(v))
		for _, t := range v {
			in = append(in, int16(t.Unix()/secInDay))
		}
		dt.values, nulls = append(dt.values, in...), make([]uint8, len(v))
	case []*time.Time:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				dt.values = append(dt.values, int16(v.Unix()/secInDay))
			default:
				dt.values, nulls[i] = append(dt.values, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Date",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (dt *Date) AppendRow(v interface{}) error {
	var date int16
	switch v := v.(type) {
	case time.Time:
		date = int16(v.Unix() / secInDay)
	case *time.Time:
		if v != nil {
			date = int16(v.Unix() / secInDay)
		}
	case nil:
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Date",
			From: fmt.Sprintf("%T", v),
		}
	}
	dt.values = append(dt.values, date)
	return nil
}

func (dt *Date) Decode(decoder *binary.Decoder, rows int) error {
	return dt.values.Decode(decoder, rows)
}

func (dt *Date) Encode(encoder *binary.Encoder) error {
	return dt.values.Encode(encoder)
}

func (dt *Date) row(i int) time.Time {
	return time.Unix(int64(dt.values[i])*secInDay, 0).UTC()
}

var _ Interface = (*Date)(nil)
