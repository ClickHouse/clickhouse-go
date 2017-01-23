package clickhouse

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"time"
)

type byteReader struct{ io.Reader }

func (b *byteReader) ReadByte() (byte, error) {
	bytes, err := readFixed(b, 1)
	if err != nil {
		return 0x0, err
	}
	return bytes[0], nil
}

func readUvarint(conn io.Reader) (uint64, error) {
	return binary.ReadUvarint(&byteReader{conn})
}

func readFixed(conn io.Reader, len int) ([]byte, error) {
	buf := make([]byte, len)
	if _, err := conn.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func readBool(conn io.Reader) (bool, error) {
	b, err := readFixed(conn, 1)
	if err != nil {
		return false, err
	}
	return b[0] == 1, nil
}

func readString(conn io.Reader) (string, error) {
	len, err := readUvarint(conn)
	if err != nil {
		return "", err
	}
	str, err := readFixed(conn, int(len))
	if err != nil {
		return "", err
	}
	return string(str), nil
}

func readUInt64(conn io.Reader) (uint64, error) {
	value, err := read(conn, "UInt64")
	if err != nil {
		return 0, err
	}
	return value.(uint64), nil
}

func readInt32(conn io.Reader) (int32, error) {
	value, err := read(conn, "Int32")
	if err != nil {
		return 0, err
	}
	return value.(int32), nil
}

var arrayTypes = map[string]interface{}{
	"Int8":     []int8{},
	"Int16":    []int16{},
	"Int32":    []int32{},
	"Int64":    []int64{},
	"UInt8":    []uint8{},
	"UInt16":   []uint16{},
	"UInt32":   []uint32{},
	"UInt64":   []uint64{},
	"Float32":  []float32{},
	"Float64":  []float64{},
	"String":   []string{},
	"Date":     []time.Time{},
	"DateTime": []time.Time{},
}

func sliceType(columnType string) (interface{}, error) {
	if strings.HasPrefix(columnType, "FixedString") {
		return arrayTypes["String"], nil
	}
	if slice, found := arrayTypes[columnType]; found {
		return slice, nil
	}
	return nil, fmt.Errorf("unsupported array type '%s", columnType)
}

func readArray(conn io.Reader, ct string, sliceLen uint64) (interface{}, error) {
	var (
		err        error
		columnType = ct[6:][:len(ct)-7]
	)
	sliceType, err := sliceType(columnType)
	if err != nil {
		return nil, err
	}
	slice := reflect.MakeSlice(reflect.TypeOf(sliceType), 0, int(sliceLen))
	for i := 0; i < int(sliceLen); i++ {
		value, err := read(conn, columnType)
		if err != nil {
			return nil, err
		}
		slice = reflect.Append(slice, reflect.ValueOf(value))
	}
	return slice.Interface(), nil
}
func read(conn io.Reader, columnType string) (interface{}, error) {
	switch {
	case strings.HasPrefix(columnType, "FixedString"):
		var len int
		if _, err := fmt.Sscanf(columnType, "FixedString(%d)", &len); err != nil {
			return nil, err
		}
		str, err := readFixed(conn, len)
		if err != nil {
			return nil, err
		}
		return string(str), nil
	}
	switch columnType {
	case "Int8":
		var value int8
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case "Int16":
		var value int16
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case "Int32":
		var value int32
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case "Int64":
		var value int64
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case "UInt8":
		var value uint8
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case "UInt16":
		var value uint16
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case "UInt32":
		var value uint32
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case "UInt64":
		var value uint64
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil

	case "Float32":
		x, err := read(conn, "UInt32")
		if err != nil {
			return nil, err
		}
		return math.Float32frombits(x.(uint32)), nil
	case "Float64":
		x, err := read(conn, "UInt64")
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(x.(uint64)), nil
	case "String":
		return readString(conn)
	case "Date":
		var sec int16
		if err := binary.Read(conn, binary.LittleEndian, &sec); err != nil {
			return nil, err
		}
		return time.Unix(int64(sec)*24*3600, 0), nil
	case "DateTime":
		var sec int32
		if err := binary.Read(conn, binary.LittleEndian, &sec); err != nil {
			return nil, err
		}
		return time.Unix(int64(sec), 0), nil
	default:
		return nil, fmt.Errorf("type '%s' is not supported", columnType)
	}
}
