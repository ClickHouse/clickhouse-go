// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
	name     string
}

func (col *Array) Name() string {
	return col.name
}

func (col *Array) parse(t Type) (_ Interface, err error) {
	col.chType = t
	var typeStr = string(t)

parse:
	for {
		switch {
		case strings.HasPrefix(typeStr, "Array("):
			col.depth++
			typeStr = strings.TrimPrefix(typeStr, "Array(")
			typeStr = strings.TrimSuffix(typeStr, ")")
		default:
			break parse
		}
	}
	if col.depth != 0 {
		if col.values, err = Type(typeStr).Column(col.name); err != nil {
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
	return nil, &UnsupportedColumnTypeError{
		t: t,
	}
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
		return len(col.offsets[0].values.data)
	}
	return 0
}

func (col *Array) Row(i int, ptr bool) interface{} {
	return col.make(uint64(i), 0).Interface()
}

func (col *Array) ScanRow(dest interface{}, row int) error {
	elem := reflect.Indirect(reflect.ValueOf(dest))
	if elem.Type() != col.scanType {
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: string(col.chType),
			Hint: fmt.Sprintf("try using *%s", col.scanType),
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
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   string(col.chType),
			From: fmt.Sprintf("%T", v),
			Hint: "value must be a slice",
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
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   string(col.chType),
			From: from,
			Hint: fmt.Sprintf("try using %s", col.scanType),
		}
	}
	return col.append(elem, 0)
}

func (col *Array) append(elem reflect.Value, level int) error {
	if level < col.depth {
		offset := uint64(elem.Len())
		if ln := len(col.offsets[level].values.data); ln != 0 {
			offset += col.offsets[level].values.data[ln-1]
		}
		col.offsets[level].values.data = append(col.offsets[level].values.data, offset)
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
		switch {
		case len(offset.values.data) > 0:
			rows = int(offset.values.data[len(offset.values.data)-1])
		default:
			rows = 0
		}
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

func (col *Array) ReadStatePrefix(decoder *binary.Decoder) error {
	if serialize, ok := col.values.(CustomSerialization); ok {
		if err := serialize.ReadStatePrefix(decoder); err != nil {
			return err
		}
	}
	return nil
}

func (col *Array) WriteStatePrefix(encoder *binary.Encoder) error {
	if serialize, ok := col.values.(CustomSerialization); ok {
		if err := serialize.WriteStatePrefix(encoder); err != nil {
			return err
		}
	}
	return nil
}

func (col *Array) make(row uint64, level int) reflect.Value {
	offset := col.offsets[level]
	var (
		end   = offset.values.data[row]
		start = uint64(0)
	)
	if row > 0 {
		start = offset.values.data[row-1]
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

func (col *Array) scanJSONSlice(sliceType reflect.Type, row int, level int) (reflect.Value, error) {
	// We could try and set - if it exceeds just return immediately
	offset := col.offsets[level]
	var (
		end   = offset.values.data[row]
		start = uint64(0)
	)
	if row > 0 {
		start = offset.values.data[row-1]
	}
	var jsonSlice reflect.Value
	switch sliceType.Kind() {
	case reflect.Interface:
		jsonSlice = reflect.ValueOf(make([]interface{}, 0, int(end-start)))
	case reflect.Slice:
		jsonSlice = reflect.MakeSlice(sliceType, 0, int(end-start))
	default:
		return reflect.Value{}, &Error{
			ColumnType: fmt.Sprint(sliceType.Kind()),
			Err:        fmt.Errorf("column %s - needs a slice or interface{}", col.Name()),
		}
	}

	for i := start; i < end; i++ {
		var value reflect.Value
		var err error
		switch {
		case level == len(col.offsets)-1:
			switch dcol := col.values.(type) {
			case *Nested:
				aCol := dcol.Interface.(*Array)
				value, err = aCol.parseJSONSliceOfObjects(sliceType.Elem(), int(i))
				if err != nil {
					return reflect.Value{}, err
				}
			case *Array:
				value, err = dcol.scanJSONSlice(sliceType.Elem(), int(i), 0)
				if err != nil {
					return reflect.Value{}, err
				}
			default:
				v := col.values.Row(int(i), false)
				if sliceType.Kind() == reflect.Interface {
					value = reflect.New(sliceType).Elem()
					if err := setJSONFieldValue(value, reflect.ValueOf(v)); err != nil {
						return reflect.Value{}, err
					}
				} else {
					value = reflect.New(sliceType.Elem()).Elem()
					if err := setJSONFieldValue(value, reflect.ValueOf(v)); err != nil {
						return reflect.Value{}, err
					}
				}
			}
		default:
			value, err = col.scanJSONSlice(sliceType.Elem(), int(i), level+1)
			if err != nil {
				return reflect.Value{}, err
			}
		}
		jsonSlice = reflect.Append(jsonSlice, value)
	}
	return jsonSlice, nil
}

func (col *Array) parseJSONSliceOfObjects(sliceType reflect.Type, row int) (reflect.Value, error) {
	if sliceType.Kind() == reflect.Interface {
		// catches interface{} - Note this swallows custom interfaces to which maps couldn't conform
		subMap := make(map[string]interface{})
		return col.scanJSONSliceOfMaps(reflect.SliceOf(reflect.TypeOf(subMap)), row)
	} else if sliceType.Kind() == reflect.Slice {
		// make a slice of the right type - we need this to be a slice of a type capable of taking an object as nested
		switch sliceType.Elem().Kind() {
		case reflect.Struct:
			return col.scanJSONSliceOfStructs(sliceType, row)
		case reflect.Map:
			return col.scanJSONSliceOfMaps(sliceType, row)
		case reflect.Interface:
			// catches []interface{} - Note this swallows custom interfaces to which maps couldn't conform
			// might lead to fail
			subMap := make(map[string]interface{})
			return col.scanJSONSliceOfMaps(reflect.SliceOf(reflect.TypeOf(subMap)), row)
		default:
			return reflect.Value{}, &Error{
				ColumnType: fmt.Sprint(sliceType.Elem().Kind()),
				Err:        fmt.Errorf("column %s - needs a slice of objects or an interface{}", col.Name()),
			}
		}
		return reflect.Value{}, nil
	}
	return reflect.Value{}, &Error{
		ColumnType: fmt.Sprint(sliceType.Kind()),
		Err:        fmt.Errorf("column %s - needs a slice or interface{}", col.Name()),
	}
}

// the following 2 functions can probably be refactored - the share alot of common code for structs and maps
func (col *Array) scanJSONSliceOfMaps(sliceType reflect.Type, row int) (reflect.Value, error) {
	if sliceType.Kind() != reflect.Slice {
		return reflect.Value{}, &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%s", sliceType),
			From: string(col.Type()),
		}
	}
	tCol, ok := col.values.(*Tuple)
	if !ok {
		return reflect.Value{}, &Error{
			ColumnType: fmt.Sprint(col.values.Type()),
			Err:        fmt.Errorf("column %s - must be a tuple", col.Name()),
		}
	}
	// Array(Tuple so depth 1 for JSON
	offset := col.offsets[0]
	var (
		end   = offset.values.data[row]
		start = uint64(0)
	)
	if row > 0 {
		start = offset.values.data[row-1]
	}
	if end-start > 0 {
		jsonSlice := reflect.MakeSlice(sliceType, 0, int(end-start))
		for i := start; i < end; i++ {
			sMap := reflect.MakeMap(sliceType.Elem())
			if err := tCol.scanJSONMap(sMap, int(i)); err != nil {
				return reflect.Value{}, err
			}
			jsonSlice = reflect.Append(jsonSlice, sMap)
		}
		return jsonSlice, nil
	}
	return reflect.MakeSlice(sliceType, 0, 0), nil
}

func (col *Array) scanJSONSliceOfStructs(sliceType reflect.Type, row int) (reflect.Value, error) {
	if sliceType.Kind() != reflect.Slice {
		return reflect.Value{}, &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%s", sliceType),
			From: string(col.Type()),
		}
	}
	tCol, ok := col.values.(*Tuple)
	if !ok {
		return reflect.Value{}, &Error{
			ColumnType: fmt.Sprint(col.values.Type()),
			Err:        fmt.Errorf("column %s - must be a tuple", col.Name()),
		}
	}
	// Array(Tuple so depth 1 for JSON
	offset := col.offsets[0]
	var (
		end   = offset.values.data[row]
		start = uint64(0)
	)
	if row > 0 {
		start = offset.values.data[row-1]
	}
	if end-start > 0 {
		// create a slice of the type from the jsonSlice - if this might be interface{} as its driven by the target datastructure
		jsonSlice := reflect.MakeSlice(sliceType, 0, int(end-start))
		for i := start; i < end; i++ {
			sStruct := reflect.New(sliceType.Elem()).Elem()
			if err := tCol.scanJSONStruct(sStruct, int(i)); err != nil {
				return reflect.Value{}, err
			}
			jsonSlice = reflect.Append(jsonSlice, sStruct)
		}
		return jsonSlice, nil
	}
	return reflect.MakeSlice(sliceType, 0, 0), nil
}

var (
	_ Interface           = (*Array)(nil)
	_ CustomSerialization = (*Array)(nil)
)
