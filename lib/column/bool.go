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

type Bool struct {
	values UInt8
	name   string
}

func (col *Bool) Name() string {
	return col.name
}

func (col *Bool) Type() Type {
	return "Bool"
}

func (col *Bool) ScanType() reflect.Type {
	return scanTypeBool
}

func (col *Bool) Rows() int {
	return len(col.values.data)
}

func (col *Bool) Row(i int, ptr bool) interface{} {
	val := col.row(i)
	if ptr {
		return &val
	}
	return val
}

func (col *Bool) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *bool:
		*d = col.row(row)
	case **bool:
		*d = new(bool)
		**d = col.row(row)
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Bool",
		}
	}
	return nil
}

func (col *Bool) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []bool:
		in := make([]uint8, 0, len(v))
		for _, v := range v {
			switch {
			case v:
				in = append(in, 1)
			default:
				in = append(in, 0)
			}
		}
		col.values.data, nulls = append(col.values.data, in...), make([]uint8, len(v))
	case []*bool:
		nulls = make([]uint8, len(v))
		in := make([]uint8, 0, len(v))
		for i, v := range v {
			var value uint8
			switch {
			case v != nil:
				if *v {
					value = 1
				}
			default:
				nulls[i] = 1
			}
			in = append(in, value)
		}
		col.values.data = append(col.values.data, in...)
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Bool",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Bool) AppendRow(v interface{}) error {
	var value bool
	switch v := v.(type) {
	case bool:
		value = v
	case *bool:
		if v != nil {
			value = *v
		}
	case nil:
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Bool",
			From: fmt.Sprintf("%T", v),
		}
	}
	switch {
	case value:
		col.values.data = append(col.values.data, 1)
	default:
		col.values.data = append(col.values.data, 0)
	}
	return nil
}

func (col *Bool) Decode(decoder *binary.Decoder, rows int) error {
	return col.values.Decode(decoder, rows)
}

func (col *Bool) Encode(encoder *binary.Encoder) error {
	return col.values.Encode(encoder)
}

func (col *Bool) row(i int) bool {
	return col.values.data[i] == 1
}

var _ Interface = (*Bool)(nil)
