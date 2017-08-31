package types

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"

	"github.com/kshvakov/clickhouse/lib/binary"
	"github.com/kshvakov/clickhouse/lib/column"
	"github.com/kshvakov/clickhouse/lib/writebuffer"
)

func NewArray(v interface{}) *Array {
	if column, ok := columnsMap[reflect.TypeOf(v)]; ok {
		return &Array{
			values: v,
			column: column,
		}
	}
	return &Array{
		err: fmt.Errorf("unsupported array type %T", v),
	}
}

func NewArrayByType(chType string, v interface{}) *Array {
	timezone := time.Local
	if tm, ok := v.(time.Time); ok {
		timezone = tm.Location()
	}
	column, err := column.Factory("", chType, timezone)
	return &Array{
		err:    err,
		values: v,
		column: column,
	}
}

type Array struct {
	err    error
	values interface{}
	column column.Column
}

func (array *Array) Value() (driver.Value, error) {
	if array.err != nil {
		return nil, array.err
	}
	var (
		v       = reflect.ValueOf(array.values)
		ln      = v.Len()
		buff    = writebuffer.New((2 * ln) + 8)
		encoder = binary.NewEncoder(buff)
	)
	encoder.Uvarint(uint64(ln))
	for i := 0; i < ln; i++ {
		if err := array.column.Write(encoder, v.Index(i).Interface()); err != nil {
			buff.Reset()
			return nil, err
		}
	}
	return buff.Bytes(), nil
}

func (array *Array) WriteArray(encoder *binary.Encoder, column column.Column) (uint64, error) {
	if array.err != nil {
		return 0, array.err
	}
	var (
		v  = reflect.ValueOf(array.values)
		ln = v.Len()
	)
	for i := 0; i < ln; i++ {
		if err := column.Write(encoder, v.Index(i).Interface()); err != nil {
			return 0, err
		}
	}
	return uint64(ln), nil
}

var columnsMap = map[reflect.Type]column.Column{
	reflect.TypeOf([]int8{}):    &column.Int8{},
	reflect.TypeOf([]int16{}):   &column.Int16{},
	reflect.TypeOf([]int32{}):   &column.Int32{},
	reflect.TypeOf([]int64{}):   &column.Int64{},
	reflect.TypeOf([]uint8{}):   &column.UInt8{},
	reflect.TypeOf([]uint16{}):  &column.UInt16{},
	reflect.TypeOf([]uint32{}):  &column.UInt32{},
	reflect.TypeOf([]uint64{}):  &column.UInt64{},
	reflect.TypeOf([]float32{}): &column.Float32{},
	reflect.TypeOf([]float64{}): &column.Float64{},
	reflect.TypeOf([]string{}):  &column.String{},
	reflect.TypeOf([]time.Time{}): &column.DateTime{
		IsFull:   true,
		Timezone: time.Local,
	},
}
