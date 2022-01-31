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
	minDate, _ = time.Parse("2006-01-02 15:04:05", "1970-01-01 00:00:00")
	maxDate, _ = time.Parse("2006-01-02 15:04:05", "2106-01-01 00:00:00")
)

type Date struct {
	values Int16
}

func (dt *Date) Type() Type {
	return "Date"
}

func (col *Date) ScanType() reflect.Type {
	return scanTypeTime
}

func (dt *Date) Rows() int {
	return len(dt.values)
}

func (dt *Date) Row(i int, ptr bool) interface{} {
	value := dt.row(i)
	if ptr {
		return &value
	}
	return value
}

func (dt *Date) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *time.Time:
		*d = dt.row(row)
	case **time.Time:
		*d = new(time.Time)
		**d = dt.row(row)
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Date",
		}
	}
	return nil
}

func (dt *Date) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []time.Time:
		in := make([]int16, 0, len(v))
		for _, t := range v {
			if err := dateOverflow(minDate, maxDate, t, "2006-01-02"); err != nil {
				return nil, err
			}
			in = append(in, int16(t.Unix()/secInDay))
		}
		dt.values, nulls = append(dt.values, in...), make([]uint8, len(v))
	case []*time.Time:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				if err := dateOverflow(minDate, maxDate, *v, "2006-01-02"); err != nil {
					return nil, err
				}
				dt.values = append(dt.values, int16(v.Unix()/secInDay))
			default:
				dt.values, nulls[i] = append(dt.values, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Date",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (dt *Date) AppendRow(v interface{}) error {
	var date int16
	switch v := v.(type) {
	case time.Time:
		if err := dateOverflow(minDate, maxDate, v, "2006-01-02"); err != nil {
			return err
		}
		date = int16(v.Unix() / secInDay)
	case *time.Time:
		if v != nil {
			if err := dateOverflow(minDate, maxDate, *v, "2006-01-02"); err != nil {
				return err
			}
			date = int16(v.Unix() / secInDay)
		}
	case nil:
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Date",
			From: fmt.Sprintf("%T", v),
		}
	}
	dt.values = append(dt.values, date)
	return nil
}

func (dt *Date) Decode(decoder *binary.Decoder, rows int) error {
	return dt.values.Decode(decoder, rows)
}

func (dt *Date) Encode(encoder *binary.Encoder) error {
	return dt.values.Encode(encoder)
}

func (dt *Date) row(i int) time.Time {
	return time.Unix(int64(dt.values[i])*secInDay, 0).UTC()
}

var _ Interface = (*Date)(nil)
