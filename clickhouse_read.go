package clickhouse

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
)

func (ch *clickhouse) read(columnType string) (driver.Value, error) {
	if strings.HasPrefix(columnType, "FixedString") {
		var len int
		if _, err := fmt.Sscanf(columnType, "FixedString(%d)", &len); err != nil {
			return nil, err
		}
		return ch.conn.readFixed(len)
	}
	switch columnType {
	case "Int8":
		var v int8
		if err := binary.Read(ch.conn, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case "Int16":
		var v int16
		if err := binary.Read(ch.conn, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case "Int32":
		var v int32
		if err := binary.Read(ch.conn, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case "Int64":
		var v int64
		if err := binary.Read(ch.conn, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case "UInt8":
		var v uint8
		if err := binary.Read(ch.conn, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case "UInt16":
		var v uint16
		if err := binary.Read(ch.conn, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case "UInt32":
		var v uint32
		if err := binary.Read(ch.conn, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case "UInt64":
		var v uint64
		if err := binary.Read(ch.conn, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case "Float32":
		x, err := ch.read("UInt32")
		if err != nil {
			return nil, err
		}
		return math.Float32frombits(x.(uint32)), nil
	case "Float64":
		x, err := ch.read("UInt64")
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(x.(uint64)), nil
	case "String":
		return ch.conn.readString()
	case "Date":
		var sec int16
		if err := binary.Read(ch.conn, binary.LittleEndian, &sec); err != nil {
			return nil, err
		}
		return time.Unix(int64(sec)*24*3600, 0).In(ch.serverTimezone), nil
	case "DateTime":
		var sec int32
		if err := binary.Read(ch.conn, binary.LittleEndian, &sec); err != nil {
			return nil, err
		}
		return time.Unix(int64(sec), 0).In(ch.serverTimezone), nil
	default:
		return nil, fmt.Errorf("unexpected type: %s", columnType)
	}
}
