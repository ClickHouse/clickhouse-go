package column

import (
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

// unix time of 1925-01-01
const date32Epoch = -1420070400

type Date32 struct {
	values Int32
}

func (dt *Date32) Type() Type {
	return "Date32"
}

func (dt *Date32) Rows() int {
	return len(dt.values)
}

func (dt *Date32) RowValue(row int) interface{} {
	return dt.row(row)
}

func (dt *Date32) ScanRow(dest interface{}, row int) error {
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
			from: "Date32",
		}
	}
	return nil
}

func (dt *Date32) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case time.Time:
		dt.values = append(dt.values, timeToInt32(v))
	case *time.Time:
		switch {
		case v == nil:
			dt.values = append(dt.values, timeToInt32(*v))
		default:
			dt.values = append(dt.values, 0)
		}
	case null:
		dt.values = append(dt.values, 0)
	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   "Date32",
			from: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (dt *Date32) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []time.Time:
		in := make([]int32, 0, len(v))
		for _, t := range v {
			in = append(in, timeToInt32(t))
		}
		dt.values, nulls = append(dt.values, in...), make([]uint8, len(v))
	case []*time.Time:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				dt.values = append(dt.values, timeToInt32(*v))
			default:
				dt.values, nulls[i] = append(dt.values, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   "Date32",
			from: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (dt *Date32) Decode(decoder *binary.Decoder, rows int) error {
	return dt.values.Decode(decoder, rows)
}

func (dt *Date32) Encode(encoder *binary.Encoder) error {
	return dt.values.Encode(encoder)
}

func (dt *Date32) row(row int) time.Time {
	return time.Unix((int64(dt.values[row])*secInDay)+date32Epoch, 0).UTC()
}

func timeToInt32(t time.Time) int32 {
	return int32((t.Unix() - date32Epoch) / secInDay)
}

var _ Interface = (*Date32)(nil)
