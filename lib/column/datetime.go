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
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"github.com/ClickHouse/clickhouse-go/v2/lib/timezone"
)

var (
	minDateTime, _ = time.Parse("2006-01-02 15:04:05", "1970-01-01 00:00:00")
	maxDateTime, _ = time.Parse("2006-01-02 15:04:05", "2105-12-31 23:59:59")
)

type DateTime struct {
	chType   Type
	values   UInt32
	timezone *time.Location
	name     string
}

func (col *DateTime) Name() string {
	return col.name
}

func (col *DateTime) parse(t Type) (_ *DateTime, err error) {
	if col.chType = t; col.chType == "DateTime" {
		return col, nil
	}
	var name = strings.TrimSuffix(strings.TrimPrefix(string(t), "DateTime('"), "')")
	if col.timezone, err = timezone.Load(name); err != nil {
		return nil, err
	}
	return col, nil
}

func (col *DateTime) Type() Type {
	return col.chType
}

func (col *DateTime) ScanType() reflect.Type {
	return scanTypeTime
}

func (col *DateTime) Rows() int {
	return len(col.values.data)
}

func (col *DateTime) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *DateTime) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *time.Time:
		*d = col.row(row)
	case **time.Time:
		*d = new(time.Time)
		**d = col.row(row)
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "DateTime",
		}
	}
	return nil
}

func (col *DateTime) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []time.Time:
		in := make([]uint32, 0, len(v))
		for _, t := range v {
			if err := dateOverflow(minDateTime, maxDateTime, t, "2006-01-02 15:04:05"); err != nil {
				return nil, err
			}
			in = append(in, uint32(t.Unix()))
		}
		col.values.data, nulls = append(col.values.data, in...), make([]uint8, len(v))
	case []*time.Time:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				if err := dateOverflow(minDateTime, maxDateTime, *v, "2006-01-02 15:04:05"); err != nil {
					return nil, err
				}
				col.values.data = append(col.values.data, uint32(v.Unix()))
			default:
				col.values.data, nulls[i] = append(col.values.data, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "DateTime",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *DateTime) AppendRow(v interface{}) error {
	var datetime uint32
	switch v := v.(type) {
	case time.Time:
		if err := dateOverflow(minDateTime, maxDateTime, v, "2006-01-02 15:04:05"); err != nil {
			return err
		}
		datetime = uint32(v.Unix())
	case *time.Time:
		if v != nil {
			if err := dateOverflow(minDateTime, maxDateTime, *v, "2006-01-02 15:04:05"); err != nil {
				return err
			}
			datetime = uint32(v.Unix())
		}
	case nil:
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "DateTime",
			From: fmt.Sprintf("%T", v),
		}
	}
	col.values.data = append(col.values.data, datetime)
	return nil
}

func (col *DateTime) Decode(decoder *binary.Decoder, rows int) error {
	return col.values.Decode(decoder, rows)
}

func (col *DateTime) Encode(encoder *binary.Encoder) error {
	return col.values.Encode(encoder)
}

func (col *DateTime) row(i int) time.Time {
	v := time.Unix(int64(col.values.data[i]), 0)
	if col.timezone != nil {
		v = v.In(col.timezone)
	}
	return v
}

var _ Interface = (*DateTime)(nil)
