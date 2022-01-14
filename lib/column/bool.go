package column

import (
	"fmt"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Bool struct {
	values UInt8
}

func (dt *Bool) Type() Type {
	return "Bool"
}

func (col *Bool) ScanType() reflect.Type {
	return scanTypeBool
}

func (dt *Bool) Rows() int {
	return len(dt.values)
}

func (dt *Bool) Row(i int) interface{} {
	return dt.row(i)
}

func (dt *Bool) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *bool:
		*d = dt.row(row)
	case **bool:
		*d = new(bool)
		**d = dt.row(row)
	default:
		return &ColumnConverterErr{
			op:   "ScanRow",
			to:   fmt.Sprintf("%T", dest),
			from: "Bool",
		}
	}
	return nil
}

func (dt *Bool) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case bool:
		switch {
		case v:
			dt.values = append(dt.values, 1)
		default:
			dt.values = append(dt.values, 0)
		}
	case *bool:
		switch {
		case v != nil:
			switch {
			case *v:
				dt.values = append(dt.values, 1)
			default:
				dt.values = append(dt.values, 0)
			}
		default:
			dt.values = append(dt.values, 0)
		}
	case null:
		dt.values = append(dt.values, 0)
	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   "Bool",
			from: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (dt *Bool) Append(v interface{}) (nulls []uint8, err error) {
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
		dt.values = append(dt.values, in...)
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
		dt.values = append(dt.values, in...)
	default:
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   "Bool",
			from: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (dt *Bool) Decode(decoder *binary.Decoder, rows int) error {
	return dt.values.Decode(decoder, rows)
}

func (dt *Bool) Encode(encoder *binary.Encoder) error {
	return dt.values.Encode(encoder)
}

func (dt *Bool) row(i int) bool {
	return dt.values[i] == 1
}

var _ Interface = (*Bool)(nil)
