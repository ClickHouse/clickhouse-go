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
	"database/sql"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"reflect"
	"time"
)

var (
	minDate, _ = time.Parse("2006-01-02 15:04:05", "1970-01-01 00:00:00")
	maxDate, _ = time.Parse("2006-01-02 15:04:05", "2106-01-01 00:00:00")
)

const (
	defaultDateFormat = "2006-01-02"
)

type Date struct {
	col  proto.ColDate
	name string
}

func (col *Date) Reset() {
	col.col.Reset()
}

func (col *Date) Name() string {
	return col.name
}

func (col *Date) Type() Type {
	return "Date"
}

func (col *Date) ScanType() reflect.Type {
	return scanTypeTime
}

func (col *Date) Rows() int {
	return col.col.Rows()
}

func (col *Date) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *Date) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *time.Time:
		*d = col.row(row)
	case **time.Time:
		*d = new(time.Time)
		**d = col.row(row)
	case *sql.NullTime:
		d.Scan(col.row(row))
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Date",
		}
	}
	return nil
}

func (col *Date) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []time.Time:
		for _, t := range v {
			if err := dateOverflow(minDate, maxDate, t, defaultDateFormat); err != nil {
				return nil, err
			}
			col.col.Append(t)
		}
	case []*time.Time:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			switch {
			case v != nil:
				if err := dateOverflow(minDate, maxDate, *v, defaultDateFormat); err != nil {
					return nil, err
				}
				col.col.Append(*v)
			default:
				nulls[i] = 1
				col.col.Append(time.Time{})
			}
		}
	case []sql.NullTime:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.AppendRow(v[i])
		}
	case []*sql.NullTime:
		nulls = make([]uint8, len(v))
		for i := range v {
			if v[i] == nil {
				nulls[i] = 1
			}
			col.AppendRow(v[i])
		}
	case []string:
		nulls = make([]uint8, len(v))
		for i := range v {
			value, err := col.parseDate(v[i])
			if err != nil {
				return nil, err
			}
			col.col.Append(value)
		}
	case []*string:
		nulls = make([]uint8, len(v))
		for i := range v {
			if v[i] == nil || *v[i] == "" {
				nulls[i] = 1
				col.col.Append(time.Time{})
			} else {
				value, err := col.parseDate(*v[i])
				if err != nil {
					return nil, err
				}
				col.col.Append(value)
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

func (col *Date) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case time.Time:
		if err := dateOverflow(minDate, maxDate, v, defaultDateFormat); err != nil {
			return err
		}
		col.col.Append(v)
	case *time.Time:
		switch {
		case v != nil:
			if err := dateOverflow(minDate, maxDate, *v, defaultDateFormat); err != nil {
				return err
			}
			col.col.Append(*v)
		default:
			col.col.Append(time.Time{})
		}
	case sql.NullTime:
		switch v.Valid {
		case true:
			col.col.Append(v.Time)
		default:
			col.col.Append(time.Time{})
		}
	case *sql.NullTime:
		switch v.Valid {
		case true:
			col.col.Append(v.Time)
		default:
			col.col.Append(time.Time{})
		}
	case nil:
		col.col.Append(time.Time{})
	case string:
		datetime, err := col.parseDate(v)
		if err != nil {
			return err
		}
		col.col.Append(datetime)
	case *string:
		if v == nil || *v == "" {
			col.col.Append(time.Time{})
		} else {
			datetime, err := col.parseDate(*v)
			if err != nil {
				return err
			}
			col.col.Append(datetime)
		}
	default:
		s, ok := v.(fmt.Stringer)
		if ok {
			return col.AppendRow(s.String())
		}
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Date",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *Date) parseDate(str string) (datetime time.Time, err error) {
	defer func() {
		if err == nil {
			err = dateOverflow(minDate, maxDate, datetime, defaultDateFormat)
		}
	}()
	return time.Parse(defaultDateFormat, str)
}

func (col *Date) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *Date) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *Date) row(i int) time.Time {
	return col.col.Row(i)
}

var _ Interface = (*Date)(nil)
