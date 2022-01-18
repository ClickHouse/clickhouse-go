package column

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type offset struct {
	values   UInt64
	scanType reflect.Type
}

type Array struct {
	depth    int
	chType   Type
	values   Interface
	offsets  []*offset
	scanType reflect.Type
}

func (col *Array) parse(t Type) (_ Interface, err error) {
	col.chType = t
	var typeStr = string(t)

parse:
	for _, str := range strings.Split(typeStr, "Array(") {
		switch {
		case len(str) == 0:
			col.depth++
		default:
			typeStr = str[:len(str)-col.depth]
			break parse
		}
	}
	if col.depth != 0 {
		if col.values, err = Type(typeStr).Column(); err != nil {
			return nil, err
		}
		offsetScanTypes := make([]reflect.Type, 0, col.depth)
		col.offsets, col.scanType = make([]*offset, 0, col.depth), col.values.ScanType()
		for i := 0; i < col.depth; i++ {
			col.scanType = reflect.SliceOf(col.scanType)
			offsetScanTypes = append(offsetScanTypes, col.scanType)
		}
		for i := len(offsetScanTypes) - 1; i >= 0; i-- {
			col.offsets = append(col.offsets, &offset{
				scanType: offsetScanTypes[i],
			})
		}
		return col, nil
	}
	return &UnsupportedColumnType{
		t: t,
	}, nil
}

func (col *Array) Base() Interface {
	return col.values
}

func (col *Array) Type() Type {
	return col.chType
}

func (col *Array) ScanType() reflect.Type {
	return col.scanType
}

func (col *Array) Rows() int {
	if len(col.offsets) != 0 {
		return len(col.offsets[0].values)
	}
	return 0
}

func (col *Array) Row(i int, ptr bool) interface{} {
	return col.make(uint64(i), 0).Interface()
}

func (col *Array) ScanRow(dest interface{}, row int) error {
	elem := reflect.Indirect(reflect.ValueOf(dest))
	if elem.Type() != col.scanType {
		return &ColumnConverterErr{
			op:     "ScanRow",
			to:     fmt.Sprintf("%T", dest),
			from:   string(col.chType),
			advise: fmt.Sprintf("use *%s", col.scanType),
		}
	}
	{
		elem.Set(col.make(uint64(row), 0))
	}
	return nil
}

func (col *Array) Append(v interface{}) (nulls []uint8, err error) {
	value := reflect.Indirect(reflect.ValueOf(v))
	if value.Kind() != reflect.Slice {
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   string(col.chType),
			from: fmt.Sprintf("%T", v),
		}
	}
	for i := 0; i < value.Len(); i++ {
		if err := col.AppendRow(value.Index(i)); err != nil {
			return nil, err
		}
	}
	return
}

func (col *Array) AppendRow(v interface{}) error {
	var elem reflect.Value
	switch v := v.(type) {
	case reflect.Value:
		elem = reflect.Indirect(v)
	default:
		elem = reflect.Indirect(reflect.ValueOf(v))
	}
	if !elem.IsValid() || elem.Type() != col.scanType {
		from := fmt.Sprintf("%T", v)
		if !elem.IsValid() {
			from = fmt.Sprintf("%v", v)
		}
		return &ColumnConverterErr{
			op:   "AppendRow",
			to:   string(col.chType),
			from: from,
		}
	}
	return col.append(elem, 0)
}

func (col *Array) append(elem reflect.Value, level int) error {
	isSlice := elem.Kind() == reflect.Slice
	switch elem.Type() {
	case scanTypeIP /*, scanTypeByte*/ :
		isSlice = false
	}
	if isSlice && level < col.depth {
		offset := uint64(elem.Len())
		if ln := len(col.offsets[level].values); ln != 0 {
			offset += col.offsets[level].values[ln-1]
		}
		col.offsets[level].values = append(col.offsets[level].values, offset)
		for i := 0; i < elem.Len(); i++ {
			if err := col.append(elem.Index(i), level+1); err != nil {
				return err
			}
		}
		return nil
	}
	if elem.Kind() == reflect.Ptr && elem.IsNil() {
		return col.values.AppendRow(nil)
	}
	return col.values.AppendRow(elem.Interface())
}

func (col *Array) Decode(decoder *binary.Decoder, rows int) error {
	for _, offset := range col.offsets {
		if err := offset.values.Decode(decoder, rows); err != nil {
			return err
		}
		rows = int(offset.values[len(offset.values)-1])
	}
	return col.values.Decode(decoder, rows)
}

func (col *Array) Encode(encoder *binary.Encoder) error {
	for _, offset := range col.offsets {
		if err := offset.values.Encode(encoder); err != nil {
			return err
		}
	}
	return col.values.Encode(encoder)
}

func (col *Array) make(row uint64, level int) reflect.Value {
	offset := col.offsets[level]
	var (
		end   = offset.values[row]
		start = uint64(0)
	)
	if row > 0 {
		start = offset.values[row-1]
	}
	var (
		base  = offset.scanType.Elem()
		isPtr = base.Kind() == reflect.Ptr
		slice = reflect.MakeSlice(offset.scanType, 0, int(end-start))
	)
	for i := start; i < end; i++ {
		var value reflect.Value
		switch {
		case level == len(col.offsets)-1:
			switch v := col.values.Row(int(i), isPtr); {
			case v == nil:
				value = reflect.Zero(base)
			default:
				value = reflect.ValueOf(v)
			}
		default:
			value = col.make(i, level+1)
		}
		slice = reflect.Append(slice, value)
	}
	return slice
}

var _ Interface = (*Array)(nil)
