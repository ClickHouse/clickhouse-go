package column

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnMap.cpp
type Map struct {
	keys     Interface
	values   Interface
	chType   Type
	offsets  Int64
	scanType reflect.Type
}

func (col *Map) parse(t Type) (_ Interface, err error) {
	col.chType = t
	if types := strings.Split(t.params(), ","); len(types) == 2 {
		if col.keys, err = Type(strings.TrimSpace(types[0])).Column(); err != nil {
			return nil, err
		}
		if col.values, err = Type(strings.TrimSpace(types[1])).Column(); err != nil {
			return nil, err
		}
		col.scanType = reflect.MapOf(
			col.keys.ScanType(),
			col.values.ScanType(),
		)
		return col, nil
	}
	return &UnsupportedColumnType{
		t: t,
	}, nil
}

func (col *Map) Type() Type {
	return col.chType
}

func (col *Map) ScanType() reflect.Type {
	return col.scanType
}

func (col *Map) Rows() int {
	return len(col.offsets)
}

func (col *Map) Row(i int) interface{} {
	return col.row(i).Interface()
}

func (col *Map) ScanRow(dest interface{}, i int) error {
	value := reflect.Indirect(reflect.ValueOf(dest))
	if value.Kind() != reflect.Map {
		return &ColumnConverterErr{
			op:   "ScanRow",
			to:   fmt.Sprintf("%T", dest),
			from: string(col.chType),
		}
	}
	{
		value.Set(col.row(i))
	}
	return nil
}

func (col *Map) Append(v interface{}) (nulls []uint8, err error) {
	value := reflect.Indirect(reflect.ValueOf(v))
	if value.Kind() != reflect.Slice {
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   string(col.chType),
			from: fmt.Sprintf("%T", v),
		}
	}
	for i := 0; i < value.Len(); i++ {
		if err := col.AppendRow(value.Index(i).Interface()); err != nil {
			return nil, err
		}
	}
	return
}

func (col *Map) AppendRow(v interface{}) error {
	value := reflect.Indirect(reflect.ValueOf(v))
	if value.Kind() != reflect.Map {
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   string(col.chType),
			from: fmt.Sprintf("%T", v),
		}
	}
	var (
		size int64
		iter = value.MapRange()
	)
	for iter.Next() {
		size++
		if err := col.keys.AppendRow(iter.Key().Interface()); err != nil {
			return err
		}
		if err := col.values.AppendRow(iter.Value().Interface()); err != nil {
			return err
		}
	}
	var prev int64
	if n := len(col.offsets); n != 0 {
		prev = col.offsets[n-1]
	}
	col.offsets = append(col.offsets, prev+size)
	return nil
}

func (col *Map) Decode(decoder *binary.Decoder, rows int) error {
	if err := col.offsets.Decode(decoder, rows); err != nil {
		return err
	}
	size := int(col.offsets[len(col.offsets)-1])
	if err := col.keys.Decode(decoder, size); err != nil {
		return err
	}
	return col.values.Decode(decoder, size)
}

func (col *Map) Encode(encoder *binary.Encoder) error {
	if err := col.offsets.Encode(encoder); err != nil {
		return err
	}
	if err := col.keys.Encode(encoder); err != nil {
		return err
	}
	return col.values.Encode(encoder)
}

func (col *Map) row(n int) reflect.Value {
	var (
		prev  int64
		value = reflect.MakeMap(col.scanType)
	)
	if n != 0 {
		prev = col.offsets[n-1]
	}
	size := int(col.offsets[n] - prev)
	for next := 0; next < size; next++ {
		value.SetMapIndex(
			reflect.ValueOf(col.keys.Row(n*size+next)),
			reflect.ValueOf(col.values.Row(n*size+next)),
		)
	}
	return value
}

var _ Interface = (*Map)(nil)
