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

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Enum8 struct {
	iv     map[string]uint8
	vi     map[uint8]string
	chType Type
	values UInt8
	name   string
}

func (col *Enum8) Name() string {
	return col.name
}

func (col *Enum8) Type() Type {
	return col.chType
}

func (col *Enum8) ScanType() reflect.Type {
	return scanTypeString
}

func (col *Enum8) Rows() int {
	return len(col.values.data)
}

func (col *Enum8) Row(i int, ptr bool) interface{} {
	value := col.vi[col.values.data[i]]
	if ptr {
		return &value
	}
	return value
}

func (col *Enum8) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = col.vi[col.values.data[row]]
	case **string:
		*d = new(string)
		**d = col.vi[col.values.data[row]]
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Enum8",
		}
	}
	return nil
}

func (col *Enum8) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []string:
		nulls = make([]uint8, len(v))
		for _, elem := range v {
			v, ok := col.iv[elem]
			if !ok {
				return nil, &Error{
					Err:        fmt.Errorf("unknown element %q", elem),
					ColumnType: string(col.chType),
				}
			}
			col.values.data = append(col.values.data, v)
		}
	case []*string:
		nulls = make([]uint8, len(v))
		for i, elem := range v {
			switch {
			case elem != nil:
				v, ok := col.iv[*elem]
				if !ok {
					return nil, &Error{
						Err:        fmt.Errorf("unknown element %q", *elem),
						ColumnType: string(col.chType),
					}
				}
				col.values.data = append(col.values.data, v)
			default:
				col.values.data, nulls[i] = append(col.values.data, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Enum8",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Enum8) AppendRow(elem interface{}) error {
	switch elem := elem.(type) {
	case string:
		v, ok := col.iv[elem]
		if !ok {
			return &Error{
				Err:        fmt.Errorf("unknown element %q", elem),
				ColumnType: string(col.chType),
			}
		}
		col.values.data = append(col.values.data, v)
	case *string:
		switch {
		case elem != nil:
			v, ok := col.iv[*elem]
			if !ok {
				return &Error{
					Err:        fmt.Errorf("unknown element %q", *elem),
					ColumnType: string(col.chType),
				}
			}
			col.values.data = append(col.values.data, v)
		default:
			col.values.data = append(col.values.data, 0)
		}
	case nil:
		col.values.data = append(col.values.data, 0)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Enum8",
			From: fmt.Sprintf("%T", elem),
		}
	}
	return nil
}

func (col *Enum8) Decode(decoder *binary.Decoder, rows int) error {
	return col.values.Decode(decoder, rows)
}

func (col *Enum8) Encode(encoder *binary.Encoder) error {
	return col.values.Encode(encoder)
}

var _ Interface = (*Enum8)(nil)
