package column

import (
	"fmt"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"github.com/google/uuid"
)

const uuidSize = 16

type UUID struct {
	data []byte
}

func (col *UUID) Type() Type {
	return "UUID"
}

func (col *UUID) ScanType() reflect.Type {
	return scanTypeUUID
}

func (col *UUID) Rows() int {
	return len(col.data) / uuidSize
}

// sql.Scanner
func (col *UUID) Row(i int) interface{} {
	return col.row(i)
}

func (col *UUID) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *uuid.UUID:
		var v uuid.UUID
		copy(v[:], col.row(row))
		*d = v
	case **uuid.UUID:
		var v uuid.UUID
		copy(v[:], col.row(row))
		*d = new(uuid.UUID)
		**d = v
	default:
		return &ColumnConverterErr{
			op:   "ScanRow",
			to:   fmt.Sprintf("%T", dest),
			from: "UUID",
		}
	}
	return nil
}

func (col *UUID) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []uuid.UUID:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			col.data = append(col.data, v[:]...)
		}
	case []*uuid.UUID:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				tmp := *v
				col.data = append(col.data, tmp[:]...)
			default:
				col.data, nulls[i] = append(col.data, make([]byte, uuidSize)...), 1
			}
		}
	default:
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   "UUID",
			from: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *UUID) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case uuid.UUID:
		col.data = append(col.data, v[:]...)
	case null:
		col.data = append(col.data, make([]byte, uuidSize)...)
	default:
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   "UUID",
			from: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *UUID) Decode(decoder *binary.Decoder, rows int) error {
	col.data = make([]byte, uuidSize*rows)
	return decoder.Raw(col.data)
}

func (col *UUID) Encode(encoder *binary.Encoder) error {
	return encoder.Raw(col.data)
}

func (col *UUID) row(i int) []byte {
	return col.data[i*uuidSize : (i+1)*uuidSize]
}

var _ Interface = (*UUID)(nil)
