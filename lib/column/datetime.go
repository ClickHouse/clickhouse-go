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
	"github.com/ClickHouse/ch-go/proto"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/timezone"
)

var (
	minDateTime, _ = time.Parse("2006-01-02 15:04:05", "1970-01-01 00:00:00")
	maxDateTime, _ = time.Parse("2006-01-02 15:04:05", "2105-12-31 23:59:59")
)

const (
	dateTimeFormat = "2006-01-02 15:04:05"
)

type DateTime struct {
	chType Type
	name   string
	col    proto.ColDateTime
}

func (col *DateTime) Name() string {
	return col.name
}

func (col *DateTime) parse(t Type) (_ *DateTime, err error) {
	if col.chType = t; col.chType == "DateTime" {
		return col, nil
	}
	var name = strings.TrimSuffix(strings.TrimPrefix(string(t), "DateTime('"), "')")
	timezone, err := timezone.Load(name)
	if err != nil {
		return nil, err
	}
	col.col.Location = timezone
	return col, nil
}

func (col *DateTime) Type() Type {
	return col.chType
}

func (col *DateTime) ScanType() reflect.Type {
	return scanTypeTime
}

func (col *DateTime) Rows() int {
	return col.col.Rows()
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
		nulls = make([]uint8, len(v))
		for i := range v {
			if err := dateOverflow(minDateTime, maxDateTime, v[i], "2006-01-02 15:04:05"); err != nil {
				return nil, err
			}
			col.col.Append(v[i])
		}

	case []*time.Time:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				if err := dateOverflow(minDateTime, maxDateTime, *v[i], "2006-01-02 15:04:05"); err != nil {
					return nil, err
				}
				col.col.Append(*v[i])
			default:
				nulls[i] = 1
				col.col.Append(time.Time{})
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
	switch v := v.(type) {
	case time.Time:
		if err := dateOverflow(minDateTime, maxDateTime, v, "2006-01-02 15:04:05"); err != nil {
			return err
		}
		col.col.Append(v)
	case *time.Time:
		switch {
		case v != nil:
			if err := dateOverflow(minDateTime, maxDateTime, *v, "2006-01-02 15:04:05"); err != nil {
				return err
			}
			col.col.Append(*v)
		default:
			col.col.Append(time.Time{})
		}
	case nil:
		col.col.Append(time.Time{})
	case string:
		dateTime, err := col.parseTime(v)
		if err != nil {
			return err
		}
		err = dateOverflow(minDateTime, maxDateTime, dateTime, "2006-01-02 15:04:05")
		if err != nil {
			return err
		}
		col.col.Append(dateTime)
	case *string:
		switch {
		case v == nil:
			col.col.Append(time.Time{})
			return nil
		case v != nil:
			dateTime, err := col.parseTime(*v)
			if err != nil {
				return err
			}
			err = dateOverflow(minDateTime, maxDateTime, dateTime, "2006-01-02 15:04:05")
			if err != nil {
				return err
			}
			col.col.Append(dateTime)
		}
	default:
		timestamp := col.convToInt64(v)
		if timestamp != 0 {
			col.col.Append(time.Unix(timestamp, 0))
			return nil
		}
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "DateTime",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *DateTime) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *DateTime) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *DateTime) row(i int) time.Time {
	v := col.col.Row(i)
	return v
}

func (col *DateTime) parseTime(str string) (time.Time, error) {
	// try if you can convert to numbers first
	timestamp := col.convToInt64(str)
	if timestamp != 0 {
		return time.Unix(timestamp, 0), nil
	}
	dateTime, err := time.Parse(dateTimeFormat, str)
	if err != nil {
		return time.Time{}, err
	}
	return dateTime, nil
}

func (col *DateTime) convToInt64(v interface{}) int64 {
	switch s := v.(type) {
	case int:
		return int64(s)
	case int8:
		return int64(s)
	case int16:
		return int64(s)
	case int32:
		return int64(s)
	case int64:
		return s
	case uint:
		return int64(s)
	case uint8:
		return int64(s)
	case uint16:
		return int64(s)
	case uint32:
		return int64(s)
	case uint64:
		return int64(s)
	case string:
		timestamp, _ := strconv.ParseInt(s, 10, 64)
		return timestamp
	case *string:
		if s == nil {
			return 0
		}
		timestamp, _ := strconv.ParseInt(*s, 10, 64)
		return timestamp
	default:
		return 0
	}
}

var _ Interface = (*DateTime)(nil)
