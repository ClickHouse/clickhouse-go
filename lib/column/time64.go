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

const defaultTime64Format = "15:04:05.999999999"

type Time64 struct {
	chType Type
	name   string
	col    proto.ColTime64
}

func (col *Time64) Reset() {
	col.col.Reset()
}

func (col *Time64) Name() string {
	return col.name
}

func (col *Time64) parse(t Type) (_ Interface, err error) {
	col.chType = t
	// if no precision is given say just Time64 (instead of Time64(3|6|9))
	// it is treated as 3 (milliseconds)
	precision := int64(3)

	if strings.HasPrefix(string(t), "Time64(") {
		params := strings.TrimSuffix(strings.TrimPrefix(string(t), "Time64("), ")")
		precision, err = strconv.ParseInt(params, 10, 8)
		if err != nil {
			return nil, err
		}
	}
	p := byte(precision)
	col.col.WithPrecision(proto.Precision(p))
	return col, nil

}

func (col *Time64) Type() Type {
	return col.chType
}

func (col *Time64) ScanType() reflect.Type {
	return scanTypeTime
}

func (col *Time64) Precision() (int64, bool) {
	return int64(col.col.Precision), col.col.PrecisionSet
}

func (col *Time64) Rows() int {
	return col.col.Rows()
}

func (col *Time64) Row(i int, ptr bool) any {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *Time64) ScanRow(dest any, row int) error {
	switch d := dest.(type) {
	case *time.Time:
		*d = col.row(row)
	case **time.Time:
		*d = new(time.Time)
		**d = col.row(row)
	case *int64:
		t := col.row(row)
		*d = timeToMilliSeconds(t)
	case **int64:
		*d = new(int64)
		t := col.row(row)
		**d = timeToMilliSeconds(t)
	case *sql.NullTime:
		return d.Scan(col.row(row))
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(col.row(row))
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Time64",
		}
	}
	return nil
}

func (col *Time64) Append(v any) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []int64:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(proto.FromTime64(milliSecondsToTime(v[i])))
		}
	case []*int64:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(proto.FromTime64(milliSecondsToTime(*v[i])))
			default:
				col.col.Append(proto.FromTime64(time.Time{}))
				nulls[i] = 1
			}
		}
	case []time.Time:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(proto.FromTime64(v[i]))
		}
	case []*time.Time:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(proto.FromTime64(*v[i]))
			default:
				col.col.Append(proto.FromTime64(time.Time{}))
				nulls[i] = 1
			}
		}
	case []string:
		nulls = make([]uint8, len(v))
		for i := range v {
			value, err := col.parseTime(v[i])
			if err != nil {
				return nil, err
			}
			col.col.Append(proto.FromTime64(value))
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
	default:
		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "Time64",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.Append(val)
		}
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Time64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Time64) AppendRow(v any) error {
	switch v := v.(type) {
	case int64:
		col.col.Append(proto.FromTime64(milliSecondsToTime(v)))
	case *int64:
		switch {
		case v != nil:
			col.col.Append(proto.FromTime64(milliSecondsToTime(*v)))
		default:
			col.col.Append(proto.FromTime64(time.Time{}))
		}
	case time.Time:
		col.col.Append(proto.FromTime64(v))
	case *time.Time:
		switch {
		case v != nil:
			col.col.Append(proto.FromTime64(*v))
		default:
			col.col.Append(proto.FromTime64(time.Time{}))
		}
	case sql.NullTime:
		if v.Valid {
			col.col.Append(proto.FromTime64(v.Time))
		} else {
			col.col.Append(proto.FromTime64(time.Time{}))
		}
	case *sql.NullTime:
		switch {
		case v != nil && v.Valid:
			col.col.Append(proto.FromTime64(v.Time))
		default:
			col.col.Append(proto.FromTime64(time.Time{}))
		}
	case string:
		value, err := col.parseTime(v)
		if err != nil {
			return err
		}
		col.col.Append(proto.FromTime64(value))
	case *string:
		switch {
		case v != nil:
			value, err := col.parseTime(*v)
			if err != nil {
				return err
			}
			col.col.Append(proto.FromTime64(value))
		default:
			col.col.Append(proto.FromTime64(time.Time{}))
		}
	default:
		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "Time64",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.AppendRow(val)
		}
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Time64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *Time64) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *Time64) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *Time64) row(i int) time.Time {
	return col.col.Row(i).ToTime()
}

func (col *Time64) parseTime(value string) (tv time.Time, err error) {
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
		"15:04:05.999999 -07:00",
		"15:04:05.999999999 -07:00",
	}

	for _, format := range formats {
		if tv, err = time.Parse(format, value); err == nil {
			return time.Date(1970, 1, 1, tv.Hour(), tv.Minute(), tv.Second(), tv.Nanosecond(), time.UTC), nil
		}
	}

	if milliseconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		return milliSecondsToTime(milliseconds), nil
	}

	return time.Time{}, fmt.Errorf("cannot parse time64 value: %s", value)
}

// helpers
func milliSecondsToTime(ms int64) time.Time {
	seconds := ms / 1000
	hours := seconds / 3600
	minutes := (seconds % 3600) / 60
	secs := seconds % 60
	nsecs := (ms % 1000) * 1000000

	return time.Date(1970, 1, 1, int(hours), int(minutes), int(secs), int(nsecs), time.UTC)
}

func timeToMilliSeconds(t time.Time) int64 {
	return int64(t.Hour()*3600000 + t.Minute()*60000 + t.Second()*1000 + t.Nanosecond()/1000000)
}
