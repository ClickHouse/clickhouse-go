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
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"github.com/ClickHouse/clickhouse-go/v2/lib/timezone"
)

var (
	minDateTime64, _ = time.Parse("2006-01-02 15:04:05", "1925-01-01 00:00:00")
	maxDateTime64, _ = time.Parse("2006-01-02 15:04:05", "2283-11-11 00:00:00")
)

type DateTime64 struct {
	chType    Type
	values    Int64
	timezone  *time.Location
	precision int
	name      string
}

func (col *DateTime64) Name() string {
	return col.name
}

func (col *DateTime64) parse(t Type) (_ Interface, err error) {
	col.chType = t
	switch params := strings.Split(t.params(), ","); len(params) {
	case 2:
		if col.precision, err = strconv.Atoi(params[0]); err != nil {
			return nil, err
		}
		if col.timezone, err = timezone.Load(params[1][2 : len(params[1])-1]); err != nil {
			return nil, err
		}
	case 1:
		if col.precision, err = strconv.Atoi(params[0]); err != nil {
			return nil, err
		}
	default:
		return nil, &UnsupportedColumnTypeError{
			t: t,
		}
	}
	return col, nil
}

func (col *DateTime64) Type() Type {
	return col.chType
}

func (col *DateTime64) ScanType() reflect.Type {
	return scanTypeTime
}

func (col *DateTime64) Rows() int {
	return len(col.values.data)
}

func (col *DateTime64) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *DateTime64) ScanRow(dest interface{}, row int) error {
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
			From: "Datetime64",
		}
	}
	return nil
}

func (col *DateTime64) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []int64:
		col.values.data, nulls = append(col.values.data, v...), make([]uint8, len(v))
	case []*int64:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				col.values.data = append(col.values.data, *v)
			default:
				col.values.data, nulls[i] = append(col.values.data, 0), 1
			}
		}
	case []time.Time:
		in := make([]int64, 0, len(v))
		for _, t := range v {
			if err := dateOverflow(minDateTime64, maxDateTime64, t, "2006-01-02 15:04:05"); err != nil {
				return nil, err
			}
			in = append(in, col.timeToInt64(t))
		}
		col.values.data, nulls = append(col.values.data, in...), make([]uint8, len(v))
	case []*time.Time:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				if err := dateOverflow(minDateTime64, maxDateTime64, *v, "2006-01-02 15:04:05"); err != nil {
					return nil, err
				}
				col.values.data = append(col.values.data, col.timeToInt64(*v))
			default:
				col.values.data, nulls[i] = append(col.values.data, 0), 1
			}
		}
	case []string:
		in := make([]int64, 0, len(v))
		for _, t := range v {
			value, err := col.parseString(t)
			if err != nil {
				return nil, err
			}
			in = append(in, value)
		}
		col.values.data, nulls = append(col.values.data, in...), make([]uint8, len(v))
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Datetime64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *DateTime64) AppendRow(v interface{}) error {
	var datetime int64
	switch v := v.(type) {
	case int64:
		datetime = v
	case *int64:
		if v != nil {
			datetime = *v
		}
	case time.Time:
		if err := dateOverflow(minDateTime64, maxDateTime64, v, "2006-01-02 15:04:05"); err != nil {
			return err
		}
		datetime = col.timeToInt64(v)
	case *time.Time:
		if v != nil {
			if err := dateOverflow(minDateTime64, maxDateTime64, *v, "2006-01-02 15:04:05"); err != nil {
				return err
			}
			datetime = col.timeToInt64(*v)
		}
	case string:
		var err error
		datetime, err = col.parseString(v)
		if err != nil {
			return err
		}
	case nil:
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Datetime64",
			From: fmt.Sprintf("%T", v),
		}
	}
	col.values.data = append(col.values.data, datetime)
	return nil
}

func (col *DateTime64) Decode(decoder *binary.Decoder, rows int) error {
	return col.values.Decode(decoder, rows)
}

func (col *DateTime64) Encode(encoder *binary.Encoder) error {
	return col.values.Encode(encoder)
}

func (col *DateTime64) row(i int) time.Time {
	var nano int64
	if col.precision < 19 {
		nano = col.values.data[i] * int64(math.Pow10(9-col.precision))
	}
	var (
		sec  = nano / int64(10e8)
		nsec = nano - sec*10e8
		time = time.Unix(sec, nsec)
	)
	if col.timezone != nil {
		time = time.In(col.timezone)
	}
	return time
}

func (col *DateTime64) timeToInt64(t time.Time) int64 {
	var timestamp int64
	if !t.IsZero() {
		timestamp = t.UnixNano()
	}
	return timestamp / int64(math.Pow10(9-col.precision))
}

func (col *DateTime64) parseString(value string) (int64, error) {
	tv, err := time.Parse("2006-01-02 15:04:05.999", value)
	if err != nil {
		return 0, err
	}
	// scale to the appropriate units based on the precision
	val := tv.UnixMilli() * int64(math.Pow10(col.precision-3))
	return val, nil
}

var _ Interface = (*DateTime64)(nil)
