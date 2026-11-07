package column

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
)

// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnMap.cpp
type Map struct {
	keys     Interface
	values   Interface
	chType   Type
	offsets  Int64
	scanType reflect.Type
	name     string
}

type OrderedMap interface {
	Get(key any) (any, bool)
	Put(key any, value any)
	Keys() <-chan any
}

type MapIterator interface {
	Next() bool
	Key() any
	Value() any
}

type IterableOrderedMap interface {
	Put(key any, value any)
	Iterator() MapIterator
}

func (col *Map) Reset() {
	col.keys.Reset()
	col.values.Reset()
	col.offsets.Reset()
}

func (col *Map) Name() string {
	return col.name
}

func (col *Map) parse(t Type, sc *ServerContext) (_ Interface, err error) {
	col.chType = t
	typeParams := t.params()
	idx := indexTopLevelComma(typeParams)
	if idx < 0 {
		return nil, &UnsupportedColumnTypeError{t: t}
	}
	keyType := Type(strings.TrimSpace(typeParams[:idx]))
	valueType := Type(strings.TrimSpace(typeParams[idx+1:]))
	if keyType == "" || valueType == "" {
		return nil, &UnsupportedColumnTypeError{t: t}
	}
	if col.keys, err = keyType.Column(col.name, sc); err != nil {
		return nil, err
	}
	if col.values, err = valueType.Column(col.name, sc); err != nil {
		return nil, err
	}
	if !col.keys.ScanType().Comparable() {
		return nil, &UnsupportedColumnTypeError{t: t}
	}
	col.scanType = reflect.MapOf(col.keys.ScanType(), col.values.ScanType())
	return col, nil
}

func indexTopLevelComma(s string) int {
	var (
		depth   int
		quote   byte
		escaped bool
	)
	for i := range len(s) {
		if escaped {
			escaped = false
			continue
		}
		if quote != 0 {
			switch s[i] {
			case '\\':
				escaped = true
			case quote:
				quote = 0
			}
			continue
		}
		switch s[i] {
		case '\'', '"', '`':
			quote = s[i]
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func (col *Map) Type() Type {
	return col.chType
}

func (col *Map) ScanType() reflect.Type {
	return col.scanType
}

func (col *Map) Rows() int {
	return col.offsets.col.Rows()
}

func (col *Map) Row(i int, ptr bool) any {
	return col.row(i).Interface()
}

func (col *Map) ScanRow(dest any, i int) error {
	if scanner, ok := dest.(sql.Scanner); ok {
		return scanner.Scan(col.row(i).Interface())
	}
	value := reflect.Indirect(reflect.ValueOf(dest))
	if value.Type() == col.scanType {
		value.Set(col.row(i))
		return nil
	}
	if om, ok := dest.(IterableOrderedMap); ok {
		return col.fillOrderedMap(om, reflect.TypeOf(dest), i)
	}
	if om, ok := dest.(OrderedMap); ok {
		return col.fillOrderedMap(om, reflect.TypeOf(dest), i)
	}
	if value.Kind() == reflect.Map {
		scanned, err := col.scanGoMap(value.Type(), i)
		if err != nil {
			return err
		}
		value.Set(scanned)
		return nil
	}
	return &ColumnConverterError{
		Op:   "ScanRow",
		To:   fmt.Sprintf("%T", dest),
		From: string(col.chType),
		Hint: fmt.Sprintf("try using %s", col.scanType),
	}
}

// scan builds row n as destType. Nested Map values use destType's element type
// so orderedmap destinations can keep duplicate keys.
func (col *Map) scan(destType reflect.Type, n int) (reflect.Value, error) {
	if destType.Kind() == reflect.Interface {
		return col.row(n), nil
	}
	if destType.Kind() == reflect.Pointer {
		inner, err := col.scan(destType.Elem(), n)
		if err != nil {
			return reflect.Value{}, err
		}
		ptr := reflect.New(destType.Elem())
		ptr.Elem().Set(inner)
		return ptr, nil
	}
	if isOrderedMapType(destType) {
		ptr := reflect.New(destType)
		if err := col.fillOrderedMap(ptr.Interface(), ptr.Type(), n); err != nil {
			return reflect.Value{}, err
		}
		return ptr.Elem(), nil
	}
	if destType.Kind() == reflect.Map {
		return col.scanGoMap(destType, n)
	}
	return reflect.Value{}, &ColumnConverterError{
		Op:   "ScanRow",
		To:   destType.String(),
		From: string(col.chType),
		Hint: fmt.Sprintf("try using %s", col.scanType),
	}
}

func (col *Map) fillOrderedMap(om any, destType reflect.Type, n int) error {
	put := orderedMapPut(om)
	if put == nil {
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   destType.String(),
			From: string(col.chType),
		}
	}
	valueType := orderedMapValueType(destType)
	var prev int64
	if n != 0 {
		prev = col.offsets.col.Row(n - 1)
	}
	size := int(col.offsets.col.Row(n) - prev)
	from := int(prev)
	for next := range size {
		val, err := col.scanMapValue(valueType, from+next)
		if err != nil {
			return err
		}
		put(col.keys.Row(from+next, false), val)
	}
	return nil
}

func orderedMapPut(om any) func(key, value any) {
	switch m := om.(type) {
	case IterableOrderedMap:
		return m.Put
	case OrderedMap:
		return m.Put
	default:
		return nil
	}
}

func (col *Map) scanMapValue(valueType reflect.Type, idx int) (any, error) {
	if valueType != nil {
		if nested, ok := col.values.(*Map); ok && (isOrderedMapType(valueType) || valueType.Kind() == reflect.Map) {
			v, err := nested.scan(valueType, idx)
			if err != nil {
				return nil, err
			}
			return v.Interface(), nil
		}
		if arr, ok := col.values.(*Array); ok && valueType.Kind() == reflect.Slice {
			v, err := arr.scan(valueType, idx)
			if err != nil {
				return nil, err
			}
			return v.Interface(), nil
		}
	}
	v := col.values.Row(idx, false)
	if v == nil {
		// typed nil so map/orderedmap element assignment stays valid
		if valueType != nil {
			return reflect.Zero(valueType).Interface(), nil
		}
		return reflect.Zero(col.values.ScanType()).Interface(), nil
	}
	return v, nil
}

func (col *Map) Append(v any) (nulls []uint8, err error) {
	value := reflect.Indirect(reflect.ValueOf(v))
	if value.Kind() != reflect.Slice {
		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   string(col.chType),
					From: fmt.Sprintf("%T", v),
					Hint: fmt.Sprintf("could not get driver.Valuer value, try using %s", col.scanType),
				}
			}
			return col.Append(val)
		}
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   string(col.chType),
			From: fmt.Sprintf("%T", v),
			Hint: fmt.Sprintf("try using %s", col.scanType),
		}
	}
	for i := 0; i < value.Len(); i++ {
		if err := col.AppendRow(value.Index(i).Interface()); err != nil {
			return nil, err
		}
	}
	return
}

func (col *Map) AppendRow(v any) error {
	if v == nil {
		// NOTE: successful Map.parse() make sure we have
		// valid col.scanType
		v = reflect.Zero(col.scanType).Interface()
	}

	value := reflect.Indirect(reflect.ValueOf(v))
	if value.Type() == col.scanType {
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
		if n := col.offsets.Rows(); n != 0 {
			prev = col.offsets.col.Row(n - 1)
		}
		col.offsets.col.Append(prev + size)
		return nil
	}

	if orderedMap, ok := asIterableOrderedMap(v); ok {
		var size int64
		iter := orderedMap.Iterator()
		for iter.Next() {
			key, value := iter.Key(), iter.Value()
			size++
			if err := col.keys.AppendRow(key); err != nil {
				return err
			}
			if err := col.values.AppendRow(value); err != nil {
				return err
			}
		}
		var prev int64
		if n := col.offsets.Rows(); n != 0 {
			prev = col.offsets.col.Row(n - 1)
		}
		col.offsets.col.Append(prev + size)
		return nil
	}

	if orderedMap, ok := asOrderedMap(v); ok {
		var size int64
		for key := range orderedMap.Keys() {
			value, ok := orderedMap.Get(key)
			if !ok {
				return fmt.Errorf("ordered map has key %v but no corresponding value", key)
			}
			size++
			if err := col.keys.AppendRow(key); err != nil {
				return err
			}
			if err := col.values.AppendRow(value); err != nil {
				return err
			}
		}
		var prev int64
		if n := col.offsets.Rows(); n != 0 {
			prev = col.offsets.col.Row(n - 1)
		}
		col.offsets.col.Append(prev + size)
		return nil
	}

	if valuer, ok := v.(driver.Valuer); ok {
		val, err := valuer.Value()
		if err != nil {
			return &ColumnConverterError{
				Op:   "AppendRow",
				To:   string(col.chType),
				From: fmt.Sprintf("%T", v),
				Hint: fmt.Sprintf("could not get driver.Valuer value, try using %s", col.scanType),
			}
		}
		return col.AppendRow(val)
	}

	return &ColumnConverterError{
		Op:   "AppendRow",
		To:   string(col.chType),
		From: fmt.Sprintf("%T", v),
		Hint: fmt.Sprintf("try using %s", col.scanType),
	}

}

func (col *Map) Decode(reader *proto.Reader, rows int) error {
	if err := col.offsets.col.DecodeColumn(reader, rows); err != nil {
		return err
	}
	if i := col.offsets.Rows(); i != 0 {
		size := int(col.offsets.col.Row(i - 1))
		if err := col.keys.Decode(reader, size); err != nil {
			return err
		}
		return col.values.Decode(reader, size)
	}
	return nil
}

func (col *Map) Encode(buffer *proto.Buffer) {
	col.offsets.col.EncodeColumn(buffer)
	col.keys.Encode(buffer)
	col.values.Encode(buffer)
}

func (col *Map) ReadStatePrefix(reader *proto.Reader) error {
	if serialize, ok := col.keys.(CustomSerialization); ok {
		if err := serialize.ReadStatePrefix(reader); err != nil {
			return err
		}
	}
	if serialize, ok := col.values.(CustomSerialization); ok {
		if err := serialize.ReadStatePrefix(reader); err != nil {
			return err
		}
	}
	return nil
}

func (col *Map) WriteStatePrefix(encoder *proto.Buffer) error {
	if serialize, ok := col.keys.(CustomSerialization); ok {
		if err := serialize.WriteStatePrefix(encoder); err != nil {
			return err
		}
	}
	if serialize, ok := col.values.(CustomSerialization); ok {
		if err := serialize.WriteStatePrefix(encoder); err != nil {
			return err
		}
	}
	return nil
}

func (col *Map) row(n int) reflect.Value {
	v, err := col.scanGoMap(col.scanType, n)
	if err != nil {
		// row() has no error path; fall back to an empty typed map
		return reflect.MakeMap(col.scanType)
	}
	return v
}

func (col *Map) scanGoMap(destType reflect.Type, n int) (reflect.Value, error) {
	value := reflect.MakeMap(destType)
	elemType := destType.Elem()
	var prev int64
	if n != 0 {
		prev = col.offsets.col.Row(n - 1)
	}
	size := int(col.offsets.col.Row(n) - prev)
	from := int(prev)
	for next := range size {
		mapValue, err := col.scanMapValue(elemType, from+next)
		if err != nil {
			return reflect.Value{}, err
		}
		var mapReflectValue reflect.Value
		if mapValue == nil {
			// Convert any nil to typed nil (such as nil *string) to preserve map element
			// https://github.com/ClickHouse/clickhouse-go/issues/1515
			mapReflectValue = reflect.New(elemType).Elem()
		} else {
			mapReflectValue = reflect.ValueOf(mapValue)
			if mapReflectValue.Type() != elemType && mapReflectValue.CanConvert(elemType) {
				mapReflectValue = mapReflectValue.Convert(elemType)
			}
		}
		value.SetMapIndex(
			reflect.ValueOf(col.keys.Row(from+next, false)),
			mapReflectValue,
		)
	}
	return value, nil
}

var (
	iterableOrderedMapType = reflect.TypeFor[IterableOrderedMap]()
	orderedMapIfaceType    = reflect.TypeFor[OrderedMap]()
)

func isOrderedMapType(t reflect.Type) bool {
	if t == nil {
		return false
	}
	if t.Implements(iterableOrderedMapType) || t.Implements(orderedMapIfaceType) {
		return true
	}
	if t.Kind() != reflect.Pointer {
		pt := reflect.PointerTo(t)
		return pt.Implements(iterableOrderedMapType) || pt.Implements(orderedMapIfaceType)
	}
	return false
}

// asIterableOrderedMap accepts both *orderedmap.Map and orderedmap.Map values.
// Nested map values from Iterator().Value() are non-pointers.
func asIterableOrderedMap(v any) (IterableOrderedMap, bool) {
	if om, ok := v.(IterableOrderedMap); ok {
		return om, true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer && isOrderedMapType(rv.Type()) && reflect.PointerTo(rv.Type()).Implements(iterableOrderedMapType) {
		ptr := reflect.New(rv.Type())
		ptr.Elem().Set(rv)
		return ptr.Interface().(IterableOrderedMap), true
	}
	return nil, false
}

func asOrderedMap(v any) (OrderedMap, bool) {
	if om, ok := v.(OrderedMap); ok {
		return om, true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer && isOrderedMapType(rv.Type()) && reflect.PointerTo(rv.Type()).Implements(orderedMapIfaceType) {
		ptr := reflect.New(rv.Type())
		ptr.Elem().Set(rv)
		return ptr.Interface().(OrderedMap), true
	}
	return nil, false
}

// orderedMapValueType returns V for orderedmap.Map[K,V] (a []entry[K,V] slice).
// Custom IterableOrderedMap types return nil and keep the default Row path.
func orderedMapValueType(t reflect.Type) reflect.Type {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Slice {
		return nil
	}
	entry := t.Elem()
	if entry.Kind() != reflect.Struct || entry.NumField() < 2 {
		return nil
	}
	if !strings.HasSuffix(entry.PkgPath(), "/lib/column/orderedmap") {
		return nil
	}
	return entry.Field(1).Type
}

var (
	_ Interface           = (*Map)(nil)
	_ CustomSerialization = (*Map)(nil)
)
