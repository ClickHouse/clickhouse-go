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
)

const defaultTimeFormat = "15:04:05"

type Time struct {
	chType Type
	name   string
	col    proto.ColTime
}

func (col *Time) Reset() {
	col.col.Reset()
}

func (col *Time) Name() string {
	return col.name
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
		t := col.row(row)
		*d = timeToSeconds(t)
	case **int64:
		*d = new(int64)
		t := col.row(row)
		**d = timeToSeconds(t)
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

func (col *Time) AppendRow(v any) error {
	switch v := v.(type) {
	case int64:
		col.col.Append(proto.FromTime32(secondsToTime(v)))
	case *int64:
		switch {
		case v != nil:
			col.col.Append(proto.FromTime32(secondsToTime(*v)))
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
		if v.Valid {
			col.col.Append(proto.FromTime32(v.Time))
		} else {
			col.col.Append(proto.FromTime32(time.Time{}))
		}
	case *sql.NullTime:
		switch {
		case v != nil && v.Valid:
			col.col.Append(proto.FromTime32(v.Time))
		default:
			col.col.Append(proto.FromTime32(time.Time{}))
		}
	case string:
		value, err := col.parseTime(v)
		if err != nil {
			return err
		}
		col.col.Append(proto.FromTime32(value))
	case *string:
		switch {
		case v != nil:
			value, err := col.parseTime(*v)
			if err != nil {
				return err
			}
			col.col.Append(proto.FromTime32(value))
		default:
			col.col.Append(proto.FromTime32(time.Time{}))
		}
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
	return col.col.Row(i).ToTime32()
}

func (col *Time) parseTime(value string) (tv time.Time, err error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}

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
			return time.Date(1970, 1, 1, tv.Hour(), tv.Minute(), tv.Second(), tv.Nanosecond(), time.UTC), nil
		}
	}

	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		hours := seconds / 3600
		minutes := (seconds % 3600) / 60
		secs := seconds % 60
		return time.Date(1970, 1, 1, int(hours), int(minutes), int(secs), 0, time.UTC), nil
	}

	return time.Time{}, fmt.Errorf("cannot parse time value: %s", value)
}

// helpers

func timeToSeconds(t time.Time) int64 {
	return int64(t.Hour()*3600 + t.Minute()*60 + t.Second())
}

func secondsToTime(v int64) time.Time {
	seconds := v
	hours := seconds / 3600
	minutes := (seconds % 3600) / 60
	secs := seconds % 60
	return time.Date(1970, 1, 1, int(hours), int(minutes), int(secs), 0, time.UTC)
}
