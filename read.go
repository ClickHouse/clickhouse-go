package clickhouse

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
)

func readUvariant(conn *connect) (uint64, error) {
	return binary.ReadUvarint(conn)
}

func readFixed(conn *connect, len int) ([]byte, error) {
	buf := make([]byte, len)
	if _, err := conn.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func readBool(conn *connect) (bool, error) {
	b, err := readFixed(conn, 1)
	if err != nil {
		return false, err
	}
	return b[0] == 1, nil
}

func readString(conn *connect) (string, error) {
	len, err := readUvariant(conn)
	if err != nil {
		return "", err
	}
	str, err := readFixed(conn, int(len))
	if err != nil {
		return "", err
	}
	return string(str), nil
}

func readInt32(conn *connect) (int32, error) {
	value, err := read(conn, "Int32")
	if err != nil {
		return 0, err
	}
	return value.(int32), nil
}

func read(conn *connect, columnType string) (interface{}, error) {
	if strings.HasPrefix(columnType, "FixedString") {
		var len int
		if _, err := fmt.Sscanf(columnType, "FixedString(%d)", &len); err != nil {
			return nil, err
		}
		return readFixed(conn, len)
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
		return time.Unix(int64(sec)*24*3600, 0).In(conn.timeLocation), nil
	case "DateTime":
		var sec int32
		if err := binary.Read(conn, binary.LittleEndian, &sec); err != nil {
			return nil, err
		}
		return time.Unix(int64(sec), 0).In(conn.timeLocation), nil
	default:
		return nil, fmt.Errorf("type '%s' is not supported", columnType)
	}
}
