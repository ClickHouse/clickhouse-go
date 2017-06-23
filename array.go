package clickhouse

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"
)

func Array(v interface{}) *array {
	return &array{
		values: v,
	}
}

func ArrayFixedString(len int, v interface{}) *array {
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
	baseType   interface{}
	columnType string
}

func (a *array) Value() (driver.Value, error) {
	switch a.values.(type) {
	case []string:
		if len(a.columnType) == 0 {
			a.columnType = "String"
		}
	case []float32:
		a.columnType = "Float32"
	case []float64:
		a.columnType = "Float64"
	case []int8:
		a.columnType = "Int8"
	case []int16:
		a.columnType = "Int16"
	case []int32:
		a.columnType = "Int32"
	case []int64:
		a.columnType = "Int64"
	case []uint8:
		a.columnType = "UInt8"
	case []uint16:
		a.columnType = "UInt16"
	case []uint32:
		a.columnType = "UInt32"
	case []uint64:
		a.columnType = "UInt64"
	}
	var (
		length = reflect.ValueOf(a.values).Len()
		buf    = wb(len(a.columnType) + (2 * length) + 8)
	)
	if err := writeString(buf, a.columnType); err != nil {
		return nil, err
	}
	if err := writeUvarint(buf, uint64(length)); err != nil {
		return nil, err
	}
	if _, err := a.write(nil, buf); err != nil {
		return nil, err
	}
	return buf.bytes(), nil
}

func (a *array) write(base interface{}, buf *writeBuffer) (uint64, error) {
	var (
		err        error
		elements   []interface{}
		columnType interface{}
	)
	switch v := base.(type) {
	case enum8:
		values, ok := a.values.([]string)
		if !ok {
			return 0, fmt.Errorf("invalid array(enum8) type %T", a.values)
		}
		for _, value := range values {
			value, err := enum(v).toValue(value)
			if err != nil {
				return 0, err
			}
			elements = append(elements, value)
		}
		columnType = v
	case enum16:
		values, ok := a.values.([]string)
		if !ok {
			return 0, fmt.Errorf("invalid array(enum16) type %T", a.values)
		}
		for _, value := range values {
			value, err := enum(v).toValue(value)
			if err != nil {
				return 0, err
			}
			elements = append(elements, value)
		}
		columnType = v
	default:
		switch values := a.values.(type) {
		case []time.Time:
			if len(a.columnType) == 0 {
				return 0, fmt.Errorf("unexpected column type")
			}
			columnType, err = toColumnType(a.columnType)
			if err != nil {
				return 0, err
			}
			for _, v := range values {
				elements = append(elements, v)
			}
		case []string:
			if len(a.columnType) != 0 {
				columnType, err = toColumnType(a.columnType)
				if err != nil {
					return 0, err
				}
			} else {
				columnType = string("")
			}
			for _, v := range values {
				elements = append(elements, v)
			}
		case [][]byte:
			if len(a.columnType) != 0 {
				columnType, err = toColumnType(a.columnType)
				if err != nil {
					return 0, err
				}
			} else {
				columnType = []byte{}
			}
			for _, v := range values {
				elements = append(elements, v)
			}
		case []float32:
			columnType = float32(0)
			for _, v := range values {
				elements = append(elements, float32(v))
			}
		case []float64:
			columnType = float64(0)
			for _, v := range values {
				elements = append(elements, v)
			}
		case []int8:
			columnType = int8(0)
			for _, v := range values {
				elements = append(elements, int8(v))
			}
		case []int16:
			columnType = int16(0)
			for _, v := range values {
				elements = append(elements, int16(v))
			}
		case []int32:
			columnType = int32(0)
			for _, v := range values {
				elements = append(elements, int32(v))
			}
		case []int64:
			columnType = int64(0)
			for _, v := range values {
				elements = append(elements, v)
			}
		case []uint8:
			columnType = uint8(0)
			for _, v := range values {
				elements = append(elements, uint8(v))
			}
		case []uint16:
			columnType = uint16(0)
			for _, v := range values {
				elements = append(elements, uint16(v))
			}
		case []uint32:
			columnType = uint32(0)
			for _, v := range values {
				elements = append(elements, uint32(v))
			}
		case []uint64:
			columnType = uint64(0)
			for _, v := range values {
				elements = append(elements, uint64(v))
			}
		default:
			return 0, fmt.Errorf("unsupported array type %T", a.values)
		}
	}
	for _, value := range elements {
		if err := write(buf, columnType, value); err != nil {

			return 0, err
		}
	}
	return uint64(len(elements)), nil
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
