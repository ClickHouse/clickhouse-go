package column

import (
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type SimpleAggregateFunction struct {
	base   Interface
	chType Type
}

func (col *SimpleAggregateFunction) parse(t Type) (_ Interface, err error) {
	col.chType = t
	base := strings.TrimSpace(strings.SplitN(t.params(), ",", 2)[1])
	if col.base, err = Type(base).Column(); err == nil {
		return col, nil
	}
	return &UnsupportedColumnType{
		t: t,
	}, nil
}

func (col *SimpleAggregateFunction) Type() Type {
	return col.chType
}
func (col *SimpleAggregateFunction) ScanType() reflect.Type {
	return col.base.ScanType()
}
func (col *SimpleAggregateFunction) Rows() int {
	return col.base.Rows()
}
func (col *SimpleAggregateFunction) Row(i int) interface{} {
	return col.base.Row(i)
}
func (col *SimpleAggregateFunction) ScanRow(dest interface{}, rows int) error {
	return col.base.ScanRow(dest, rows)
}
func (col *SimpleAggregateFunction) Append(v interface{}) ([]uint8, error) {
	return col.base.Append(v)
}
func (col *SimpleAggregateFunction) AppendRow(v interface{}) error {
	return col.base.AppendRow(v)
}
func (col *SimpleAggregateFunction) Decode(decoder *binary.Decoder, rows int) error {
	return col.base.Decode(decoder, rows)
}
func (col *SimpleAggregateFunction) Encode(encoder *binary.Encoder) error {
	return col.base.Encode(encoder)
}

var _ Interface = (*SimpleAggregateFunction)(nil)
