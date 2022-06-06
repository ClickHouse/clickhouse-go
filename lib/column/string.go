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
	"encoding"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"reflect"
)

type String struct {
	name string
	data []string
}

func (col String) Name() string {
	return col.name
}

func (String) Type() Type {
	return "String"
}

func (String) ScanType() reflect.Type {
	return scanTypeString
}

func (col *String) Rows() int {
	return len(col.data)
}

func (col *String) Row(i int, ptr bool) interface{} {
	value := *col
	if ptr {
		return &value.data[i]
	}
	return value.data[i]
}

func (col *String) ScanRow(dest interface{}, row int) error {
	v := *col
	switch d := dest.(type) {
	case *string:
		*d = v.data[row]
	case **string:
		*d = new(string)
		**d = v.data[row]
	case encoding.BinaryUnmarshaler:
		return d.UnmarshalBinary(binary.Str2Bytes(v.data[row]))
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "String",
		}
	}
	return nil
}

func (col *String) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []string:
		col.data, nulls = append(col.data, v...), make([]uint8, len(v))
	case []*string:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				col.data = append(col.data, *v)
			default:
				col.data, nulls[i] = append(col.data, ""), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "String",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *String) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case string:
		col.data = append(col.data, v)
	case *string:
		switch {
		case v != nil:
			col.data = append(col.data, *v)
		default:
			col.data = append(col.data, "")
		}
	case nil:
		col.data = append(col.data, "")
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "String",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *String) Decode(decoder *binary.Decoder, rows int) error {
	for i := 0; i < rows; i++ {
		v, err := decoder.String()
		if err != nil {
			return err
		}
		col.data = append(col.data, v)
	}
	return nil
}

func (col *String) Encode(encoder *binary.Encoder) error {
	for _, v := range col.data {
		if err := encoder.String(v); err != nil {
			return err
		}
	}
	return nil
}

var _ Interface = (*String)(nil)
