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

func (col *Bool) Row(i int) interface{} {
	return col.row(i)
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

func (col *Bool) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case bool:
		switch {
		case v:
			col.values = append(col.values, 1)
		default:
			col.values = append(col.values, 0)
		}
	case *bool:
		switch {
		case v != nil:
			switch {
			case *v:
				col.values = append(col.values, 1)
			default:
				col.values = append(col.values, 0)
			}
		default:
			col.values = append(col.values, 0)
		}
	case null:
		col.values = append(col.values, 0)
	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   "Bool",
			from: fmt.Sprintf("%T", v),
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
		col.values = append(col.values, in...)
	case []*bool:
		nulls = make([]uint8, len(v))
		in := make([]uint8, 0, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				switch {
				case *v:
					in = append(in, 1)
				default:
					in = append(in, 0)
				}
			default:
				in, nulls[i] = append(in, 0), 1
			}
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
