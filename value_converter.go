// +build go1.8

package clickhouse

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"reflect"
)

var _converter = &converter{}

func (stmt *stmt) ColumnConverter(idx int) driver.ValueConverter {
	return _converter
}

type converter struct{}

func (c *converter) ConvertValue(v interface{}) (driver.Value, error) {
	if driver.IsValue(v) {
		return v, nil
	}

	switch value := v.(type) {
	case int:
		return int64(value), nil
	case int8:
		return int64(value), nil
	case int16:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case int64:
		return int64(value), nil
	case uint:
		return int64(value), nil
	case uint8:
		return int64(value), nil
	case uint16:
		return int64(value), nil
	case uint32:
		return int64(value), nil
	case uint64:
		if value >= 1<<63 {
			v := make([]byte, 8)
			binary.LittleEndian.PutUint64(v, value)
			return v, nil
		}
		return int64(value), nil
	case float32:
		return float64(value), nil
	case float64:
		return value, nil
	case
		[]int, []int8, []int16, []int32, []int64,
		[]uint, []uint8, []uint16, []uint32, []uint64,
		[]string:
		return (&array{values: v}).Value()
	}

	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil, nil
		}
		return c.ConvertValue(rv.Elem().Interface())
	}

	return nil, fmt.Errorf("value converter: unsupported type %T", v)
}
