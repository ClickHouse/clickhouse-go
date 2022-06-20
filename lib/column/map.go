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

// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnMap.cpp
type Map struct {
	keys     Interface
	values   Interface
	chType   Type
	offsets  Int64
	scanType reflect.Type
	name     string
}

func (col *Map) Name() string {
	return col.name
}

func (col *Map) parse(t Type) (_ Interface, err error) {
	col.chType = t
	if types := strings.SplitN(t.params(), ",", 2); len(types) == 2 {
		if col.keys, err = Type(strings.TrimSpace(types[0])).Column(col.name); err != nil {
			return nil, err
		}
		if col.values, err = Type(strings.TrimSpace(types[1])).Column(col.name); err != nil {
			return nil, err
		}
		col.scanType = reflect.MapOf(
			col.keys.ScanType(),
			col.values.ScanType(),
		)
		return col, nil
	}
	return nil, &UnsupportedColumnTypeError{
		t: t,
	}
}

func (col *Map) Type() Type {
	return col.chType
}

func (col *Map) ScanType() reflect.Type {
	return col.scanType
}

func (col *Map) Rows() int {
	return len(col.offsets.data)
}

func (col *Map) Row(i int, ptr bool) interface{} {
	return col.row(i).Interface()
}

func (col *Map) ScanRow(dest interface{}, i int) error {
	value := reflect.Indirect(reflect.ValueOf(dest))
	if value.Type() != col.scanType {
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: string(col.chType),
			Hint: fmt.Sprintf("try using %s", col.scanType),
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

func (col *Map) AppendRow(v interface{}) error {
	value := reflect.Indirect(reflect.ValueOf(v))
	if value.Type() != col.scanType {
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   string(col.chType),
			From: fmt.Sprintf("%T", v),
			Hint: fmt.Sprintf("try using %s", col.scanType),
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
	if n := len(col.offsets.data); n != 0 {
		prev = col.offsets.data[n-1]
	}
	col.offsets.data = append(col.offsets.data, prev+size)
	return nil
}

func (col *Map) Decode(decoder *binary.Decoder, rows int) error {
	if err := col.offsets.Decode(decoder, rows); err != nil {
		return err
	}
	size := int(col.offsets.data[len(col.offsets.data)-1])
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

func (col *Map) ReadStatePrefix(decoder *binary.Decoder) error {
	if serialize, ok := col.keys.(CustomSerialization); ok {
		if err := serialize.ReadStatePrefix(decoder); err != nil {
			return err
		}
	}
	if serialize, ok := col.values.(CustomSerialization); ok {
		if err := serialize.ReadStatePrefix(decoder); err != nil {
			return err
		}
	}
	return nil
}

func (col *Map) WriteStatePrefix(encoder *binary.Encoder) error {
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
	var (
		prev  int64
		value = reflect.MakeMap(col.scanType)
	)
	if n != 0 {
		prev = col.offsets.data[n-1]
	}
	var (
		size = int(col.offsets.data[n] - prev)
		from = int(prev)
	)
	for next := 0; next < size; next++ {
		value.SetMapIndex(
			reflect.ValueOf(col.keys.Row(from+next, false)),
			reflect.ValueOf(col.values.Row(from+next, false)),
		)
	}
	return value
}

var (
	_ Interface           = (*Map)(nil)
	_ CustomSerialization = (*Map)(nil)
)
