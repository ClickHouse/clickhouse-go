package column

import (
	"fmt"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Enum8 struct {
	iv     map[string]uint8
	vi     map[uint8]string
	chType Type
	values UInt8
}

func (e *Enum8) Type() Type {
	return e.chType
}

func (col *Enum8) ScanType() reflect.Type {
	return scanTypeString
}

func (e *Enum8) Rows() int {
	return len(e.values)
}

func (e *Enum8) Row(i int, ptr bool) interface{} {
	value := e.vi[e.values[i]]
	if ptr {
		return &value
	}
	return value
}

func (e *Enum8) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = e.vi[e.values[row]]
	case **string:
		*d = new(string)
		**d = e.vi[e.values[row]]
	default:
		return &ColumnConverterErr{
			op:   "ScanRow",
			to:   fmt.Sprintf("%T", dest),
			from: "Enum8",
		}
	}
	return nil
}

func (e *Enum8) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []string:
		nulls = make([]uint8, len(v))
		for _, elem := range v {
			v, ok := e.iv[elem]
			if !ok {
				return nil, &UnknownElementForEnum{
					element: elem,
				}
			}
			e.values = append(e.values, v)
		}
	case []*string:
		nulls = make([]uint8, len(v))
		for i, elem := range v {
			switch {
			case elem != nil:
				v, ok := e.iv[*elem]
				if !ok {
					return nil, &UnknownElementForEnum{
						element: *elem,
					}
				}
				e.values = append(e.values, v)
			default:
				e.values, nulls[i] = append(e.values, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   "Enum8",
			from: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (e *Enum8) AppendRow(elem interface{}) error {
	switch elem := elem.(type) {
	case string:
		v, ok := e.iv[elem]
		if !ok {
			return &UnknownElementForEnum{
				element: elem,
			}
		}
		e.values = append(e.values, v)
	case *string:
		switch {
		case elem != nil:
			v, ok := e.iv[*elem]
			if !ok {
				return &UnknownElementForEnum{
					element: *elem,
				}
			}
			e.values = append(e.values, v)
		default:
			e.values = append(e.values, 0)
		}
	case nil:
		e.values = append(e.values, 0)
	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   "Enum8",
			from: fmt.Sprintf("%T", elem),
		}
	}
	return nil
}

func (e *Enum8) Decode(decoder *binary.Decoder, rows int) error {
	return e.values.Decode(decoder, rows)
}

func (e *Enum8) Encode(encoder *binary.Encoder) error {
	return e.values.Encode(encoder)
}

var _ Interface = (*Enum8)(nil)
