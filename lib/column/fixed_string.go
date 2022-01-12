package column

import (
	"encoding"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type FixedString struct {
	data []byte
	size int
}

func (col *FixedString) parse(t Type) (*FixedString, error) {
	if _, err := fmt.Sscanf(string(t), "FixedString(%d)", &col.size); err != nil {
		return nil, err
	}
	return col, nil
}

func (col *FixedString) Type() Type {
	return Type(fmt.Sprintf("FixedString(%d)", col.size))
}

func (col *FixedString) Rows() int {
	if col.size == 0 {
		return 0
	}
	return len(col.data) / col.size
}

func (col *FixedString) RowValue(row int) interface{} {
	return col.row(row)
}

func (col *FixedString) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *[]byte:
		*d = col.row(row)
	case **[]byte:
		*d = new([]byte)
		**d = col.row(row)
	case *string:
		*d = string(col.row(row))
	case encoding.BinaryUnmarshaler:
		return d.UnmarshalBinary(col.row(row))
	default:
		return &ColumnConverterErr{
			op:   "ScanRow",
			to:   fmt.Sprintf("%T", dest),
			from: "FixedString",
		}
	}
	return nil
}

func (col *FixedString) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []byte:
		if len(v)%col.size != 0 {
			return nil, &InvalidFixedSizeData{
				op:       "Append",
				got:      len(v),
				expected: col.size,
			}
		}
		col.data, nulls = append(col.data, v...), make([]uint8, len(v)/col.size)
	case [][]byte:
		for _, v := range v {
			if len(v) != col.size {
				return nil, &InvalidFixedSizeData{
					op:       "Append",
					got:      len(v),
					expected: col.size,
				}
			}
			col.data, nulls = append(col.data, v...), make([]uint8, len(v))
		}
	case []*[]byte:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				col.data = append(col.data, *v...)
			default:
				col.data, nulls[i] = append(col.data, make([]byte, col.size)...), 1
			}
		}
	case encoding.BinaryMarshaler:
		data, err := v.MarshalBinary()
		if err != nil {
			return nil, err
		}
		if len(data)%col.size != 0 {
			return nil, &InvalidFixedSizeData{
				op:       "Append",
				got:      len(data),
				expected: col.size,
			}
		}
		col.data, nulls = append(col.data, data...), make([]uint8, len(data)/col.size)
	default:
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   "FixedString",
			from: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *FixedString) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case []byte:
		if len(v) != col.size {
			return &InvalidFixedSizeData{
				op:       "AppendRow",
				got:      len(v),
				expected: col.size,
			}
		}
		col.data = append(col.data, v...)
	case encoding.BinaryMarshaler:
		data, err := v.MarshalBinary()
		if err != nil {
			return err
		}
		if len(data) != col.size {
			return &InvalidFixedSizeData{
				op:       "AppendRow",
				got:      len(data),
				expected: col.size,
			}
		}
		col.data = append(col.data, data...)
	case null:
		col.data = append(col.data, make([]byte, col.size)...)
	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   "FixedString",
			from: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *FixedString) Decode(decoder *binary.Decoder, rows int) error {
	col.data = make([]byte, col.size*rows)
	return decoder.Raw(col.data)
}

func (col *FixedString) Encode(encoder *binary.Encoder) error {
	return encoder.Raw(col.data)
}

func (col *FixedString) row(row int) []byte {
	return col.data[row*col.size : (row+1)*col.size]
}

var _ Interface = (*FixedString)(nil)
