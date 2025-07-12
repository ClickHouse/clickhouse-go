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
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ClickHouse/clickhouse-go/v2/lib/timezone"
)

const (
	defaultTimeFormat = "15:04:05"

	// Binary encoding constants for Time types
	binaryTypeTimeUTC          = 0x32 // Time (32-bit seconds since midnight)
	binaryTypeTimeWithTimezone = 0x33 // Time with timezone
)

// Time implements ClickHouse Time (Int32, seconds) column with optional timezone.
// Stores time-of-day only, no date component. Supports negative values and multiple input formats.
type Time struct {
	chType   Type
	timezone *time.Location
	name     string
	col      proto.ColTime
}

func (col *Time) Reset() {
	col.col.Reset()
}

func (col *Time) Name() string {
	return col.name
}

// parse parses the ClickHouse type definition and sets timezone if present.
func (col *Time) parse(t Type, tz *time.Location) (_ Interface, err error) {
	col.chType = t
	// Handle Time('UTC') format
	if strings.HasPrefix(string(t), "Time('") {
		timezoneName := strings.TrimSuffix(strings.TrimPrefix(string(t), "Time('"), "')")
		timezone, err := timezone.Load(timezoneName)
		if err != nil {
			return nil, err
		}
		col.timezone = timezone
		return col, nil
	}
	// Handle plain Time format
	if string(t) == "Time" {
		col.timezone = tz
		return col, nil
	}
	return nil, &UnsupportedColumnTypeError{
		t: t,
	}
}

func (col *Time) Type() Type {
	return col.chType
}

func (col *Time) ScanType() reflect.Type {
	return scanTypeTime
}

func (col *Time) Rows() int {
	return col.col.Rows()
}

func (col *Time) Row(i int, ptr bool) any {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *Time) ScanRow(dest any, row int) error {
	switch d := dest.(type) {
	case *time.Time:
		*d = col.row(row)
	case **time.Time:
		*d = new(time.Time)
		**d = col.row(row)
	case *int64:
		// Convert time.Time to seconds since midnight (can be negative)
		t := col.row(row)
		*d = int64(t.Hour()*3600 + t.Minute()*60 + t.Second())
	case **int64:
		*d = new(int64)
		// Convert time.Time to seconds since midnight (can be negative)
		t := col.row(row)
		**d = int64(t.Hour()*3600 + t.Minute()*60 + t.Second())
	case *sql.NullTime:
		return d.Scan(col.row(row))
	case *string:
		*d = col.row(row).Format(defaultTimeFormat)
	case **string:
		*d = new(string)
		**d = col.row(row).Format(defaultTimeFormat)
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(col.row(row))
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Time",
		}
	}
	return nil
}

func (col *Time) Append(v any) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []int64:
		nulls = make([]uint8, len(v))
		for i := range v {
			// Convert seconds since midnight to time.Time (can be negative)
			seconds := v[i]
			hours := seconds / 3600
			minutes := (seconds % 3600) / 60
			secs := seconds % 60
			col.col.Append(proto.FromTime32(time.Date(1970, 1, 1, int(hours), int(minutes), int(secs), 0, time.UTC)))
		}
	case []*int64:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				// Convert seconds since midnight to time.Time (can be negative)
				seconds := *v[i]
				hours := seconds / 3600
				minutes := (seconds % 3600) / 60
				secs := seconds % 60
				col.col.Append(proto.FromTime32(time.Date(1970, 1, 1, int(hours), int(minutes), int(secs), 0, time.UTC)))
			default:
				col.col.Append(proto.FromTime32(time.Time{}))
				nulls[i] = 1
			}
		}
	case []time.Time:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(proto.FromTime32(v[i]))
		}
	case []*time.Time:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(proto.FromTime32(*v[i]))
			default:
				col.col.Append(proto.FromTime32(time.Time{}))
				nulls[i] = 1
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
			value, err := col.parseTime(v[i])
			if err != nil {
				return nil, err
			}
			col.col.Append(proto.FromTime32(value))
		}
	default:
		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "Time",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.Append(val)
		}
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Time",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

// AppendRow appends a value to the column. Accepts time.Time, int64 (seconds), string, or driver.Valuer.
func (col *Time) AppendRow(v any) error {
	switch v := v.(type) {
	case int64:
		// Convert seconds since midnight to time.Time (can be negative)
		seconds := v
		hours := seconds / 3600
		minutes := (seconds % 3600) / 60
		secs := seconds % 60
		col.col.Append(proto.FromTime32(time.Date(1970, 1, 1, int(hours), int(minutes), int(secs), 0, time.UTC)))
	case *int64:
		switch {
		case v != nil:
			// Convert seconds since midnight to time.Time (can be negative)
			seconds := *v
			hours := seconds / 3600
			minutes := (seconds % 3600) / 60
			secs := seconds % 60
			col.col.Append(proto.FromTime32(time.Date(1970, 1, 1, int(hours), int(minutes), int(secs), 0, time.UTC)))
		default:
			col.col.Append(proto.FromTime32(time.Time{}))
		}
	case time.Time:
		col.col.Append(proto.FromTime32(v))
	case *time.Time:
		switch {
		case v != nil:
			col.col.Append(proto.FromTime32(*v))
		default:
			col.col.Append(proto.FromTime32(time.Time{}))
		}
	case sql.NullTime:
		switch v.Valid {
		case true:
			col.col.Append(proto.FromTime32(v.Time))
		default:
			col.col.Append(proto.FromTime32(time.Time{}))
		}
	case *sql.NullTime:
		switch v.Valid {
		case true:
			col.col.Append(proto.FromTime32(v.Time))
		default:
			col.col.Append(proto.FromTime32(time.Time{}))
		}
	case nil:
		col.col.Append(proto.FromTime32(time.Time{}))
	case string:
		timeValue, err := col.parseTime(v)
		if err != nil {
			return err
		}
		col.col.Append(proto.FromTime32(timeValue))
	default:
		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "Time",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.AppendRow(val)
		}
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Time",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *Time) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *Time) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *Time) row(i int) time.Time {
	t := col.col.Row(i)
	return t.ToTime32()
}

func (col *Time) parseTime(value string) (tv time.Time, err error) {
	// Try multiple time formats
	formats := []string{
		"15:04:05",
		"15:04",
		"15:04:05.999",
		"15:04:05.999999",
		"15:04:05.999999999",
		"3:04:05 PM",
		"3:04 PM",
		"15:04:05 -07:00",
		"15:04:05.999 -07:00",
	}

	for _, format := range formats {
		if tv, err = time.Parse(format, value); err == nil {
			// Extract only the time part and use the column's timezone if set
			timezone := time.UTC
			if col.timezone != nil {
				timezone = col.timezone
			}
			return time.Date(1970, 1, 1, tv.Hour(), tv.Minute(), tv.Second(), tv.Nanosecond(), timezone), nil
		}
	}

	// Try parsing as seconds since midnight
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		hours := seconds / 3600
		minutes := (seconds % 3600) / 60
		secs := seconds % 60
		timezone := time.UTC
		if col.timezone != nil {
			timezone = col.timezone
		}
		return time.Date(1970, 1, 1, int(hours), int(minutes), int(secs), 0, timezone), nil
	}

	return time.Time{}, fmt.Errorf("cannot parse time value: %s", value)
}

var _ Interface = (*Time)(nil)
var _ CustomSerialization = (*Time)(nil)

// WriteStatePrefix is a no-op for Time
func (col *Time) WriteStatePrefix(buffer *proto.Buffer) error {
	return nil
}

// ReadStatePrefix is a no-op for Time
func (col *Time) ReadStatePrefix(reader *proto.Reader) error {
	return nil
}
