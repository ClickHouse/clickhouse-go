package clickhouse

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
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
	value, err := read(conn, uint64(0))
	if err != nil {
		return 0, err
	}
	return value.(uint64), nil
}

func readInt32(conn io.Reader) (int32, error) {
	value, err := read(conn, int32(0))
	if err != nil {
		return 0, err
	}
	return value.(int32), nil
}

func readArray(conn io.Reader, columnInfo interface{}, sliceLen uint64) (interface{}, error) {
	var sliceType interface{}
	switch columnInfo.(type) {
	case int8:
		sliceType = []int8{}
	case int16:
		sliceType = []int16{}
	case int32:
		sliceType = []int32{}
	case int64:
		sliceType = []int64{}
	case uint8:
		sliceType = []uint8{}
	case uint16:
		sliceType = []uint16{}
	case uint32:
		sliceType = []uint32{}
	case uint64:
		sliceType = []uint64{}
	case float32:
		sliceType = []float32{}
	case float64:
		sliceType = []float64{}
	case string, []byte, enum8, enum16:
		sliceType = []string{}
	case Date, DateTime:
		sliceType = []time.Time{}
	default:
		return nil, fmt.Errorf("unsupported array type '%T'", columnInfo)
	}
	slice := reflect.MakeSlice(reflect.TypeOf(sliceType), 0, int(sliceLen))
	for i := 0; i < int(sliceLen); i++ {
		value, err := read(conn, columnInfo)
		if err != nil {
			return nil, err
		}
		slice = reflect.Append(slice, reflect.ValueOf(value))
	}
	return slice.Interface(), nil
}
func read(conn io.Reader, columnInfo interface{}) (interface{}, error) {
	switch value := columnInfo.(type) {
	case []byte:
		str, err := readFixed(conn, len(value))
		if err != nil {
			return nil, err
		}
		return string(str), nil
	case int8:
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case int16:
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case int32:
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case int64:
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case uint8:
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case uint16:
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case uint32:
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case uint64:
		if err := binary.Read(conn, binary.LittleEndian, &value); err != nil {
			return nil, err
		}
		return value, nil
	case float32:
		x, err := read(conn, uint32(0))
		if err != nil {
			return nil, err
		}
		return math.Float32frombits(x.(uint32)), nil
	case float64:
		x, err := read(conn, uint64(0))
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(x.(uint64)), nil
	case string:
		return readString(conn)
	case enum8:
		var ident int8
		if err := binary.Read(conn, binary.LittleEndian, &ident); err != nil {
			return nil, err
		}
		enum := enum(value)
		return enum.toIdent(ident)
	case enum16:
		var ident int16
		if err := binary.Read(conn, binary.LittleEndian, &ident); err != nil {
			return nil, err
		}
		enum := enum(value)
		return enum.toIdent(ident)
	case Date:
		var sec int16
		if err := binary.Read(conn, binary.LittleEndian, &sec); err != nil {
			return nil, err
		}
		return time.Unix(int64(sec)*24*3600, 0), nil
	case DateTime:
		var sec int32
		if err := binary.Read(conn, binary.LittleEndian, &sec); err != nil {
			return nil, err
		}
		return time.Unix(int64(sec), 0), nil
	default:
		return nil, fmt.Errorf("type '%T' is not supported", columnInfo)
	}
}
