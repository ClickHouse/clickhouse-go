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
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

var (
	minDate32, _ = time.Parse("2006-01-02 15:04:05", "1925-01-01 00:00:00")
	maxDate32, _ = time.Parse("2006-01-02 15:04:05", "2283-11-11 00:00:00")
)

type Date32 struct {
	values Int32
	name   string
}

func (col *Date32) Name() string {
	return col.name
}

func (col *Date32) Type() Type {
	return "Date32"
}

func (col *Date32) ScanType() reflect.Type {
	return scanTypeTime
}

func (col *Date32) Rows() int {
	return len(col.values.data)
}

func (col *Date32) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *Date32) ScanRow(dest interface{}, row int) error {
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
			From: "Date32",
		}
	}
	return nil
}

func (col *Date32) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []time.Time:
		in := make([]int32, 0, len(v))
		for _, t := range v {
			if err := dateOverflow(minDate32, maxDate32, t, "2006-01-02"); err != nil {
				return nil, err
			}
			in = append(in, timeToInt32(t))
		}
		col.values.data, nulls = append(col.values.data, in...), make([]uint8, len(v))
	case []*time.Time:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				if err := dateOverflow(minDate32, maxDate32, *v, "2006-01-02"); err != nil {
					return nil, err
				}
				col.values.data = append(col.values.data, timeToInt32(*v))
			default:
				col.values.data, nulls[i] = append(col.values.data, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Date32",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Date32) AppendRow(v interface{}) error {
	var date int32
	switch v := v.(type) {
	case time.Time:
		if err := dateOverflow(minDate32, maxDate32, v, "2006-01-02"); err != nil {
			return err
		}
		date = timeToInt32(v)
	case *time.Time:
		if v != nil {
			if err := dateOverflow(minDate32, maxDate32, *v, "2006-01-02"); err != nil {
				return err
			}
			date = timeToInt32(*v)
		}
	case nil:
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Date32",
			From: fmt.Sprintf("%T", v),
		}
	}
	col.values.data = append(col.values.data, date)
	return nil
}

func (col *Date32) Decode(decoder *binary.Decoder, rows int) error {
	return col.values.Decode(decoder, rows)
}

func (col *Date32) Encode(encoder *binary.Encoder) error {
	return col.values.Encode(encoder)
}

func (col *Date32) row(i int) time.Time {
	return time.Unix((int64(col.values.data[i]) * secInDay), 0).UTC()
}

func timeToInt32(t time.Time) int32 {
	return int32(t.Unix() / secInDay)
}

var _ Interface = (*Date32)(nil)
