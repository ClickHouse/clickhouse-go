package clickhouse

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Truncate timezone
//
//   clickhouse.Date(time.Date(2017, 1, 1, 0, 0, 0, 0, time.Local)) -> time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
type Date time.Time

func (date Date) Value() (driver.Value, error) {
	return time.Date(time.Time(date).Year(), time.Time(date).Month(), time.Time(date).Day(), 0, 0, 0, 0, time.UTC), nil
}

// Truncate timezone
//
//   clickhouse.DateTime(time.Date(2017, 1, 1, 0, 0, 0, 0, time.Local)) -> time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
type DateTime time.Time

func (datetime DateTime) Value() (driver.Value, error) {
	return time.Date(
		time.Time(datetime).Year(),
		time.Time(datetime).Month(),
		time.Time(datetime).Day(),
		time.Time(datetime).Hour(),
		time.Time(datetime).Minute(),
		time.Time(datetime).Second(),
		0,
		time.UTC,
	), nil
}

func isInsert(query string) bool {
	if f := strings.Fields(query); len(f) > 2 {
		return strings.EqualFold("INSERT", f[0]) && strings.EqualFold("INTO", f[1]) && strings.Index(strings.ToUpper(query), " SELECT ") == -1
	}
	return false
}

var splitInsertRe = regexp.MustCompile(`(?i)\sVALUES\s*\(`)

func formatQuery(query string) string {
	if isInsert(query) {
		return splitInsertRe.Split(query, -1)[0] + " VALUES "
	}
	return query
}

func quote(v driver.Value) string {
	switch v.(type) {
	case string, *string, time.Time, *time.Time:
		return "'" + escape(v) + "'"
	}
	return fmt.Sprint(v)
}

func escape(v driver.Value) string {
	switch value := v.(type) {
	case string:
		return strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value)
	case *string:
		return strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(*value)
	case time.Time:
		return formatTime(value)
	case *time.Time:
		return formatTime(*value)
	}
	return fmt.Sprint(v)
}

func formatTime(value time.Time) string {
	if (value.Hour() + value.Minute() + value.Second() + value.Nanosecond()) == 0 {
		return value.Format("2006-01-02")
	}
	return value.Format("2006-01-02 15:04:05")
}

func Array(v interface{}) *array {
	return &array{
		values: v,
	}
}

func ArrayFixedString(len int, v []string) *array {
	return &array{
		values:     v,
		columnType: fmt.Sprintf("FixedString(%d)", len),
	}
}

func ArrayDate(v []time.Time) *array {
	return &array{
		values:     v,
		columnType: "Date",
	}
}

func ArrayDateTime(v []time.Time) *array {
	return &array{
		values:     v,
		columnType: "DateTime",
	}
}

type array struct {
	values     interface{}
	columnType string
}

func (a *array) Value() (driver.Value, error) {
	var (
		buf      bytes.Buffer
		elements []interface{}
	)
	switch values := a.values.(type) {
	case []time.Time:
		if len(a.columnType) == 0 {
			return nil, fmt.Errorf("unexpected column type")
		}
		for _, v := range values {
			elements = append(elements, v)
		}
	case []string:
		if len(a.columnType) == 0 {
			a.columnType = "String"
		}
		for _, v := range values {
			elements = append(elements, v)
		}
	case []float32:
		a.columnType = "Float32"
		for _, v := range values {
			elements = append(elements, float64(v))
		}
	case []float64:
		a.columnType = "Float64"
		for _, v := range values {
			elements = append(elements, v)
		}
	case []int8:
		a.columnType = "Int8"
		for _, v := range values {
			elements = append(elements, int64(v))
		}
	case []int16:
		a.columnType = "Int16"
		for _, v := range values {
			elements = append(elements, int64(v))
		}
	case []int32:
		a.columnType = "Int32"
		for _, v := range values {
			elements = append(elements, int64(v))
		}
	case []int64:
		a.columnType = "Int64"
		for _, v := range values {
			elements = append(elements, v)
		}
	case []uint8:
		a.columnType = "UInt8"
		for _, v := range values {
			elements = append(elements, int64(v))
		}
	case []uint16:
		a.columnType = "UInt16"
		for _, v := range values {
			elements = append(elements, int64(v))
		}
	case []uint32:
		a.columnType = "UInt32"
		for _, v := range values {
			elements = append(elements, int64(v))
		}
	case []uint64:
		a.columnType = "UInt64"
		for _, v := range values {
			elements = append(elements, int64(v))
		}
	default:
		return nil, fmt.Errorf("unsupported array type %T", a.values)
	}
	if err := writeString(&buf, a.columnType); err != nil {
		return nil, err
	}
	if err := writeUvarint(&buf, uint64(len(elements))); err != nil {
		return nil, err
	}
	for _, value := range elements {
		if err := write(&buf, a.columnType, value); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func arrayInfo(b []byte) (string, uint64, []byte, error) {
	var (
		err        error
		arrayLen   uint64
		columnType string
		buff       = bytes.NewBuffer(b)
	)
	if columnType, err = readString(buff); err != nil {
		return "", 0, nil, err
	}
	if arrayLen, err = readUvarint(buff); err != nil {
		return "", 0, nil, err
	}
	return columnType, arrayLen, buff.Bytes(), nil
}
