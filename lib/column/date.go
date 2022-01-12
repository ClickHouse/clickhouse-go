package column

import (
	"fmt"
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

func (dt *Date) Rows() int {
	return len(dt.values)
}

func (dt *Date) RowValue(row int) interface{} {
	return dt.row(row)
}

func (dt *Date) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *time.Time:
		*d = dt.row(row)
	case **time.Time:
		*d = new(time.Time)
		**d = dt.row(row)
	default:
		return &ColumnConverterErr{
			op:   "ScanRow",
			to:   fmt.Sprintf("%T", dest),
			from: "Date",
		}
	}
	return nil
}

func (dt *Date) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case time.Time:
		dt.values = append(dt.values, int16(v.Unix()/secInDay))
	case *time.Time:
		switch {
		case v == nil:
			dt.values = append(dt.values, int16(v.Unix()/secInDay))
		default:
			dt.values = append(dt.values, 0)
		}
	case null:
		dt.values = append(dt.values, 0)
	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   "Date",
			from: fmt.Sprintf("%T", v),
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
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   "Date",
			from: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (dt *Date) Decode(decoder *binary.Decoder, rows int) error {
	return dt.values.Decode(decoder, rows)
}

func (dt *Date) Encode(encoder *binary.Encoder) error {
	return dt.values.Encode(encoder)
}

func (dt *Date) row(row int) time.Time {
	return time.Unix(int64(dt.values[row])*secInDay, 0).In(time.UTC)
}

var _ Interface = (*Date)(nil)
