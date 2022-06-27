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
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"github.com/shopspring/decimal"
)

type Decimal struct {
	chType    Type
	scale     int
	nobits    int // its domain is {32, 64, 128, 256}
	precision int
	values    []decimal.Decimal
	name      string
}

func (col *Decimal) Name() string {
	return col.name
}

func (col *Decimal) parse(t Type) (_ *Decimal, err error) {
	col.chType = t
	params := strings.Split(t.params(), ",")
	if len(params) != 2 {
		return nil, fmt.Errorf("invalid Decimal format: '%s'", t)
	}
	params[0] = strings.TrimSpace(params[0])
	params[1] = strings.TrimSpace(params[1])

	if col.precision, err = strconv.Atoi(params[0]); err != nil {
		return nil, fmt.Errorf("'%s' is not Decimal type: %s", t, err)
	} else if col.precision < 1 {
		return nil, errors.New("wrong precision of Decimal type")
	}

	if col.scale, err = strconv.Atoi(params[1]); err != nil {
		return nil, fmt.Errorf("'%s' is not Decimal type: %s", t, err)
	} else if col.scale < 0 || col.scale > col.precision {
		return nil, errors.New("wrong scale of Decimal type")
	}
	switch {
	case col.precision <= 9:
		col.nobits = 32
	case col.precision <= 18:
		col.nobits = 64
	case col.precision <= 38:
		col.nobits = 128
	default:
		col.nobits = 256
	}
	return col, nil
}

func (col *Decimal) Type() Type {
	return col.chType
}

func (col *Decimal) ScanType() reflect.Type {
	return scanTypeDecimal
}

func (col *Decimal) Rows() int {
	return len(col.values)
}

func (col *Decimal) Row(i int, ptr bool) interface{} {
	value := col.values[i]
	if ptr {
		return &value
	}
	return value
}

func (col *Decimal) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *decimal.Decimal:
		*d = col.values[row]
	case **decimal.Decimal:
		*d = new(decimal.Decimal)
		**d = col.values[row]
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Decimal",
		}
	}
	return nil
}

func (col *Decimal) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []decimal.Decimal:
		col.values, nulls = append(col.values, v...), make([]uint8, len(v))
	case []*decimal.Decimal:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				col.values = append(col.values, *v)
			default:
				col.values, nulls[i] = append(col.values, decimal.New(0, 0)), 1
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

func (col *Decimal) AppendRow(v interface{}) error {
	value := decimal.New(0, 0)
	switch v := v.(type) {
	case decimal.Decimal:
		value = v
	case *decimal.Decimal:
		if v != nil {
			value = *v
		}
	case nil:
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   string(col.chType),
			From: fmt.Sprintf("%T", v),
		}
	}
	col.values = append(col.values, value)
	return nil
}

func (col *Decimal) Decode(decoder *binary.Decoder, rows int) error {
	switch col.nobits {
	case 32:
		var base Int32
		if err := base.Decode(decoder, rows); err != nil {
			return err
		}
		for _, v := range base.data {
			col.values = append(col.values, decimal.New(int64(v), int32(-col.scale)))
		}
	case 64:
		var base Int64
		if err := base.Decode(decoder, rows); err != nil {
			return err
		}
		for _, v := range base.data {
			col.values = append(col.values, decimal.New(int64(v), int32(-col.scale)))
		}
	case 128, 256:
		var (
			size    = col.nobits / 8
			scratch = make([]byte, rows*size)
		)
		if err := decoder.Raw(scratch); err != nil {
			return err
		}
		for i := 0; i < rows; i++ {
			col.values = append(col.values, decimal.NewFromBigInt(
				rawToBigInt(scratch[i*size:(i+1)*size], true),
				int32(-col.scale),
			))
		}
	default:
		return fmt.Errorf("unsupported %s", col.chType)
	}
	return nil
}

func (col *Decimal) Encode(encoder *binary.Encoder) error {
	switch col.nobits {
	case 32:
		var base UInt32
		for _, v := range col.values {
			var part uint32
			switch {
			case v.Exponent() != int32(col.scale):
				part = uint32(decimal.NewFromBigInt(v.Coefficient(), v.Exponent()+int32(col.scale)).IntPart())
			default:
				part = uint32(v.IntPart())
			}
			base.data = append(base.data, part)
		}
		return base.Encode(encoder)
	case 64:
		var base UInt64
		for _, v := range col.values {
			var part uint64
			switch {
			case v.Exponent() != int32(col.scale):
				part = uint64(decimal.NewFromBigInt(v.Coefficient(), v.Exponent()+int32(col.scale)).IntPart())
			default:
				part = uint64(v.IntPart())
			}
			base.data = append(base.data, part)
		}
		return base.Encode(encoder)
	case 128, 256:
		var (
			size    = col.nobits / 8
			scratch = make([]byte, col.Rows()*size)
		)
		for i, v := range col.values {
			var bi *big.Int
			switch {
			case v.Exponent() != int32(col.scale):
				bi = decimal.NewFromBigInt(v.Coefficient(), v.Exponent()+int32(col.scale)).BigInt()
			default:
				bi = v.BigInt()
			}
			bigIntToRaw(scratch[i*size:(i+1)*size], bi)
		}
		return encoder.Raw(scratch)
	}
	return fmt.Errorf("unsupported %s", col.chType)
}

func (col *Decimal) Scale() int64 {
	return int64(col.scale)
}

func (col *Decimal) Precision() int64 {
	return int64(col.precision)
}

var _ Interface = (*Decimal)(nil)
