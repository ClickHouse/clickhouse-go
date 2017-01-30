package clickhouse

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"time"
)

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
	baseType   interface{}
	columnType string
}

func (a *array) Value() (driver.Value, error) {
	var elements []interface{}
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
	buf := bytes.NewBuffer(make([]byte, 0, len(a.columnType)+(2*len(elements))+8))
	if err := writeString(buf, a.columnType); err != nil {
		return nil, err
	}
	if err := writeUvarint(buf, uint64(len(elements))); err != nil {
		return nil, err
	}
	columnType, err := toColumnType(a.columnType)
	if err != nil {
		return nil, err
	}
	for _, value := range elements {
		if err := write(buf, columnType, value); err != nil {
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
