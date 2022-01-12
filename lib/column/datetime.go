package column

import (
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"github.com/ClickHouse/clickhouse-go/v2/lib/timezone"
)

type DateTime struct {
	chType   Type
	values   Int32
	timezone *time.Location
}

func (dt *DateTime) parse(t Type) (_ *DateTime, err error) {
	if dt.chType = t; dt.chType == "DateTime" {
		return dt, nil
	}
	var name = strings.TrimSuffix(strings.TrimPrefix(string(t), "DateTime('"), "')")
	if dt.timezone, err = timezone.Load(name); err != nil {
		return nil, err
	}
	return dt, nil
}

func (dt *DateTime) Type() Type {
	return dt.chType
}

func (dt *DateTime) Rows() int {
	return len(dt.values)
}

func (dt *DateTime) RowValue(row int) interface{} {
	return dt.row(row)
}

func (dt *DateTime) ScanRow(dest interface{}, row int) error {
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
			from: "DateTime",
		}
	}
	return nil
}

func (dt *DateTime) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case time.Time:
		dt.values = append(dt.values, int32(v.Unix()))
	case *time.Time:
		switch {
		case v == nil:
			dt.values = append(dt.values, int32(v.Unix()))
		default:
			dt.values = append(dt.values, 0)
		}
	case null:
		dt.values = append(dt.values, 0)
	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   "DateTime",
			from: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (dt *DateTime) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []time.Time:
		in := make([]int32, 0, len(v))
		for _, t := range v {
			in = append(in, int32(t.Unix()))
		}
		dt.values, nulls = append(dt.values, in...), make([]uint8, len(v))
	case []*time.Time:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				dt.values = append(dt.values, int32(v.Unix()))
			default:
				dt.values, nulls[i] = append(dt.values, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   "DateTime",
			from: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (dt *DateTime) Decode(decoder *binary.Decoder, rows int) error {
	return dt.values.Decode(decoder, rows)
}

func (dt *DateTime) Encode(encoder *binary.Encoder) error {
	return dt.values.Encode(encoder)
}

func (dt *DateTime) row(row int) time.Time {
	v := time.Unix(int64(dt.values[row]), 0)
	if dt.timezone != nil {
		v = v.In(dt.timezone)
	}
	return v
}

var _ Interface = (*DateTime)(nil)
