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
	"github.com/google/uuid"
)

const uuidSize = 16

type UUID struct {
	data []byte
	name string
}

func (col *UUID) Name() string {
	return col.name
}

func (col *UUID) Type() Type {
	return "UUID"
}

func (col *UUID) ScanType() reflect.Type {
	return scanTypeUUID
}

func (col *UUID) Rows() int {
	return len(col.data) / uuidSize
}

func (col *UUID) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *UUID) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = col.row(row).String()
	case **string:
		*d = new(string)
		**d = col.row(row).String()
	case *uuid.UUID:
		*d = col.row(row)
	case **uuid.UUID:
		*d = new(uuid.UUID)
		**d = col.row(row)
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "UUID",
			Hint: fmt.Sprintf("try using *%s", col.ScanType()),
		}
	}
	return nil
}

func (col *UUID) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []string:
		nulls = make([]uint8, len(v))
		var data []byte
		for _, v := range v {
			var u uuid.UUID
			u, err = uuid.Parse(v)
			if err != nil {
				return
			}
			col.data = append(col.data, swap(u[:])...)
		}
		col.data = append(col.data, data...)
	case []*string:
		nulls = make([]uint8, len(v))
		var data []byte
		for i, v := range v {
			switch {
			case v != nil:
				var tmp uuid.UUID
				tmp, err = uuid.Parse(*v)
				if err != nil {
					return
				}
				data = append(data, swap(tmp[:])...)
			default:
				data, nulls[i] = append(data, make([]byte, uuidSize)...), 1
			}
		}
		col.data = append(col.data, data...)
	case []uuid.UUID:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			col.data = append(col.data, swap(v[:])...)
		}
	case []*uuid.UUID:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				tmp := *v
				col.data = append(col.data, swap(tmp[:])...)
			default:
				col.data, nulls[i] = append(col.data, make([]byte, uuidSize)...), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "UUID",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *UUID) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case string:
		u, err := uuid.Parse(v)
		if err != nil {
			return err
		}
		col.data = append(col.data, swap(u[:])...)
	case *string:
		switch {
		case v != nil:
			tmp, err := uuid.Parse(*v)
			if err != nil {
				return err
			}
			col.data = append(col.data, swap(tmp[:])...)
		default:
			col.data = append(col.data, make([]byte, uuidSize)...)
		}
	case uuid.UUID:
		col.data = append(col.data, swap(v[:])...)
	case *uuid.UUID:
		switch {
		case v != nil:
			tmp := *v
			col.data = append(col.data, swap(tmp[:])...)
		default:
			col.data = append(col.data, make([]byte, uuidSize)...)
		}
	case nil:
		col.data = append(col.data, make([]byte, uuidSize)...)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "UUID",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *UUID) Decode(decoder *binary.Decoder, rows int) error {
	col.data = make([]byte, uuidSize*rows)
	return decoder.Raw(col.data)
}

func (col *UUID) Encode(encoder *binary.Encoder) error {
	return encoder.Raw(col.data)
}

func (col *UUID) row(i int) (uuid uuid.UUID) {
	copy(uuid[:], col.data[i*uuidSize:(i+1)*uuidSize])
	swap(uuid[:])
	return
}

var _ Interface = (*UUID)(nil)

func swap(src []byte) []byte {
	_ = src[15]
	src[0], src[7] = src[7], src[0]
	src[1], src[6] = src[6], src[1]
	src[2], src[5] = src[5], src[2]
	src[3], src[4] = src[4], src[3]
	src[8], src[15] = src[15], src[8]
	src[9], src[14] = src[14], src[9]
	src[10], src[13] = src[13], src[10]
	src[11], src[12] = src[12], src[11]
	return src
}
