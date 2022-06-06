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
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Nullable struct {
	base     Interface
	nulls    UInt8
	enable   bool
	scanType reflect.Type
	name     string
}

func (col *Nullable) Name() string {
	return col.name
}

func (col *Nullable) parse(t Type) (_ *Nullable, err error) {
	col.enable = true
	if col.base, err = Type(t.params()).Column(col.name); err != nil {
		return nil, err
	}
	switch base := col.base.ScanType(); {
	case base == nil:
		col.scanType = reflect.TypeOf(nil)
	case base.Kind() == reflect.Ptr:
		col.scanType = base
	default:
		col.scanType = reflect.New(base).Type()
	}
	return col, nil
}

func (col *Nullable) Base() Interface {
	return col.base
}

func (col *Nullable) Type() Type {
	return "Nullable(" + col.base.Type() + ")"
}

func (col *Nullable) ScanType() reflect.Type {
	return col.scanType
}

func (col *Nullable) Rows() int {
	if !col.enable {
		return col.base.Rows()
	}
	return len(col.nulls.data)
}

func (col *Nullable) Row(i int, ptr bool) interface{} {
	if col.enable {
		if col.nulls.data[i] == 1 {
			return nil
		}
	}
	return col.base.Row(i, true)
}

func (col *Nullable) ScanRow(dest interface{}, row int) error {
	if col.enable {
		if col.nulls.data[row] == 1 {
			return nil
		}
	}
	return col.base.ScanRow(dest, row)
}

func (col *Nullable) Append(v interface{}) ([]uint8, error) {
	nulls, err := col.base.Append(v)
	if err != nil {
		return nil, err
	}
	col.nulls.data = append(col.nulls.data, nulls...)
	return nulls, nil
}

func (col *Nullable) AppendRow(v interface{}) error {
	if v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil()) {
		col.nulls.data = append(col.nulls.data, 1)
	} else {
		col.nulls.data = append(col.nulls.data, 0)
	}
	return col.base.AppendRow(v)
}

func (col *Nullable) Decode(decoder *binary.Decoder, rows int) (err error) {
	if col.enable {
		if err := col.nulls.Decode(decoder, rows); err != nil {
			return err
		}
	}
	if err := col.base.Decode(decoder, rows); err != nil {
		return err
	}
	return nil
}

func (col *Nullable) Encode(encoder *binary.Encoder) error {
	if col.enable {
		if err := col.nulls.Encode(encoder); err != nil {
			return err
		}
	}
	if err := col.base.Encode(encoder); err != nil {
		return err
	}
	return nil
}

var _ Interface = (*Nullable)(nil)
