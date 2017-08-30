// +build go1.8

package clickhouse

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"net"
	"reflect"

	"github.com/kshvakov/clickhouse/lib/types"
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
		[]float32, []float64,
		[]string:
		return (types.NewArray(v)).Value()
	case net.IP:
		return IP(value).Value()
	case driver.Valuer:
		return value.Value()
	}

	switch v := v.(type) {
	case Date:
		return v.convert(), nil
	case DateTime:
		return v.convert(), nil
	default:
		switch value := reflect.ValueOf(v); value.Kind() {
		case reflect.Bool:
			if value.Bool() {
				return int64(1), nil
			}
			return int64(0), nil
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return value.Int(), nil
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return int64(value.Uint()), nil
		case reflect.Float32, reflect.Float64:
			return value.Float(), nil
		case reflect.String:
			return value.String(), nil
		}
	}

	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil, nil
		}
		return c.ConvertValue(rv.Elem().Interface())
	}

	return nil, fmt.Errorf("value converter: unsupported type %T", v)
}
