package column

import (
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Nullable struct {
	base  Interface
	nulls UInt8
}

func (col *Nullable) parse(t Type) (_ *Nullable, err error) {
	if col.base, err = Type(t.params()).Column(); err != nil {
		return nil, err
	}
	return col, nil
}

func (col *Nullable) Type() Type {
	return "Nullable(" + col.base.Type() + ")"
}

func (col *Nullable) ScanType() reflect.Type {
	return col.base.ScanType()
}

func (col *Nullable) Rows() int {
	return len(col.nulls)
}

func (col *Nullable) Row(i int) interface{} {
	if col.nulls[i] == 1 {
		return nil
	}
	return col.base.Row(i)
}

func (col *Nullable) ScanRow(dest interface{}, row int) error {
	if col.nulls[row] == 1 {
		return nil
	}
	return col.base.ScanRow(dest, row)
}

func (col *Nullable) Append(v interface{}) ([]uint8, error) {
	nulls, err := col.base.Append(v)
	if err != nil {
		return nil, err
	}
	col.nulls = append(col.nulls, nulls...)
	return nulls, nil
}

func (col *Nullable) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case nil:
		col.nulls = append(col.nulls, 1)
		return col.base.AppendRow(null{})
	default:
		col.nulls = append(col.nulls, 0)
		return col.base.AppendRow(v)
	}
}

func (col *Nullable) Decode(decoder *binary.Decoder, rows int) (err error) {
	if err := col.nulls.Decode(decoder, rows); err != nil {
		return err
	}
	if err := col.base.Decode(decoder, rows); err != nil {
		return err
	}
	return nil
}

func (col *Nullable) Encode(encoder *binary.Encoder) error {
	if err := col.nulls.Encode(encoder); err != nil {
		return err
	}
	if err := col.base.Encode(encoder); err != nil {
		return err
	}
	return nil
}

var _ Interface = (*Nullable)(nil)
