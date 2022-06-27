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
	"math/big"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type BigInt struct {
	size   int
	data   []byte
	chType Type
	name   string
	signed bool
}

func (col *BigInt) Name() string {
	return col.name
}

func (col *BigInt) Type() Type {
	return col.chType
}

func (col *BigInt) ScanType() reflect.Type {
	return scanTypeBigInt
}

func (col *BigInt) Rows() int {
	return len(col.data) / col.size
}

func (col *BigInt) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return value
	}
	return *value
}

func (col *BigInt) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *big.Int:
		*d = *col.row(row)
	case **big.Int:
		*d = new(big.Int)
		**d = *col.row(row)
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: string(col.chType),
		}
	}
	return nil
}

func (col *BigInt) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []big.Int:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			col.append(&v)
		}
	case []*big.Int:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				col.append(v)
			default:
				col.data, nulls[i] = append(col.data, make([]byte, col.size)...), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   string(col.chType),
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *BigInt) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case big.Int:
		col.append(&v)
	case *big.Int:
		switch {
		case v != nil:
			col.append(v)
		default:
			col.data = append(col.data, make([]byte, col.size)...)
		}
	case nil:
		col.data = append(col.data, make([]byte, col.size)...)
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   string(col.chType),
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *BigInt) Decode(decoder *binary.Decoder, rows int) error {
	col.data = make([]byte, rows*col.size)
	return decoder.Raw(col.data)
}

func (col *BigInt) Encode(encoder *binary.Encoder) error {
	return encoder.Raw(col.data)
}

func (col *BigInt) row(i int) *big.Int {
	return rawToBigInt(col.data[i*col.size:(i+1)*col.size], col.signed)
}

func (col *BigInt) append(v *big.Int) {
	dest := make([]byte, col.size)
	bigIntToRaw(dest, new(big.Int).Set(v))
	col.data = append(col.data, dest...)
}

func bigIntToRaw(dest []byte, v *big.Int) {
	var sign int
	if v.Sign() < 0 {
		v.Not(v).FillBytes(dest)
		sign = -1
	} else {
		v.FillBytes(dest)
	}
	endianSwap(dest, sign < 0)
}

func rawToBigInt(v []byte, signed bool) *big.Int {
	// LittleEndian to BigEndian
	endianSwap(v, false)
	var lt = new(big.Int)
	if signed && len(v) > 0 && v[0]&0x80 != 0 {
		// [0] ^ will +1
		for i := 0; i < len(v); i++ {
			v[i] = ^v[i]
		}
		lt.SetBytes(v)
		// neg ^ will -1
		lt.Not(lt)
	} else {
		lt.SetBytes(v)
	}
	return lt
}

func endianSwap(src []byte, not bool) {
	for i := 0; i < len(src)/2; i++ {
		if not {
			src[i], src[len(src)-i-1] = ^src[len(src)-i-1], ^src[i]
		} else {
			src[i], src[len(src)-i-1] = src[len(src)-i-1], src[i]
		}
	}
}

var _ Interface = (*BigInt)(nil)
