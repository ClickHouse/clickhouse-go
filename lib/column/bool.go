package column

import (
	"fmt"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Bool struct {
	values UInt8
}

func (col *Bool) Type() Type {
	return "Bool"
}

func (col *Bool) ScanType() reflect.Type {
	return scanTypeBool
}

func (col *Bool) Rows() int {
	return len(col.values)
}

func (col *Bool) Row(i int, ptr bool) interface{} {
	val := col.row(i)
	if ptr {
		return &val
	}
	return val
}

func (col *Bool) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *bool:
		*d = col.row(row)
	case **bool:
		*d = new(bool)
		**d = col.row(row)
	default:
		return &ColumnConverterErr{
			op:   "ScanRow",
			to:   fmt.Sprintf("%T", dest),
			from: "Bool",
		}
	}
	return nil
}

func (col *Bool) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []bool:
		in := make([]uint8, 0, len(v))
		for _, v := range v {
			switch {
			case v:
				in = append(in, 1)
			default:
				in = append(in, 0)
			}
		}
		col.values, nulls = append(col.values, in...), make([]uint8, len(v))
	case []*bool:
		nulls = make([]uint8, len(v))
		in := make([]uint8, 0, len(v))
		for i, v := range v {
			var value uint8
			switch {
			case v != nil:
				if *v {
					value = 1
				}
			default:
				nulls[i] = 1
			}
			in = append(in, value)
		}
		col.values = append(col.values, in...)
	default:
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   "Bool",
			from: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Bool) AppendRow(v interface{}) error {
	var value bool
	switch v := v.(type) {
	case bool:
		value = v
	case *bool:
		if v != nil {
			value = *v
		}
	case nil:
	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   "Bool",
			from: fmt.Sprintf("%T", v),
		}
	}
	switch {
	case value:
		col.values = append(col.values, 1)
	default:
		col.values = append(col.values, 0)
	}
	return nil
}

func (col *Bool) Decode(decoder *binary.Decoder, rows int) error {
	return col.values.Decode(decoder, rows)
}

func (col *Bool) Encode(encoder *binary.Encoder) error {
	return col.values.Encode(encoder)
}

func (col *Bool) row(i int) bool {
	return col.values[i] == 1
}

var _ Interface = (*Bool)(nil)
