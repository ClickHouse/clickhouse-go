package clickhouse

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

func writeBool(conn io.Writer, v bool) error {
	value := []byte{0}
	if v {
		value[0] = 1
	}
	if _, err := conn.Write(value); err != nil {
		return err
	}
	return nil
}

func writeUvarint(conn io.Writer, v uint64) error {
	var (
		buf = make([]byte, binary.MaxVarintLen64)
		len = binary.PutUvarint(buf, v)
	)
	if _, err := conn.Write(buf[0:len]); err != nil {
		return err
	}
	return nil
}

func writeString(conn io.Writer, str string) error {
	if err := writeUvarint(conn, uint64(len([]byte(str)))); err != nil {
		return err
	}
	if _, err := conn.Write([]byte(str)); err != nil {
		return err
	}
	return nil
}

func writeInt32(conn io.Writer, v int32) error {
	if err := binary.Write(conn, binary.LittleEndian, v); err != nil {
		return err
	}
	return nil
}

func writeUInt64(conn io.Writer, v uint64) error {
	if err := binary.Write(conn, binary.LittleEndian, v); err != nil {
		return err
	}
	return nil
}

func write(buffer io.Writer, columnType string, v driver.Value) error {
	switch columnType {
	case "Date", "DateTime":
		var (
			err  error
			date time.Time
		)
		switch value := v.(type) {
		case time.Time:
			date = value
		case string:
			if columnType == "Date" {
				if date, err = time.Parse("2006-01-02", value); err != nil {
					return err
				}
			} else if date, err = time.Parse("2006-01-02 15:04:05", value); err != nil {
				return err
			}
			date = time.Date(
				time.Time(date).Year(),
				time.Time(date).Month(),
				time.Time(date).Day(),
				time.Time(date).Hour(),
				time.Time(date).Minute(),
				time.Time(date).Second(),
				0,
				time.UTC,
			)
		default:
			return fmt.Errorf("unexpected type %T", v)
		}
		if columnType == "Date" {
			if err := binary.Write(buffer, binary.LittleEndian, int16(date.Unix()/24/3600)); err != nil {
				return err
			}
		} else if err := binary.Write(buffer, binary.LittleEndian, int32(date.Unix())); err != nil {
			return err
		}
		return nil
	case "String":
		var str string
		switch v := v.(type) {
		case []byte:
			str = string(v)
		case string:
			str = v
		default:
			return fmt.Errorf("unexpected type %T", v)
		}
		var (
			buf = make([]byte, binary.MaxVarintLen64)
			len = binary.PutUvarint(buf, uint64(len(str)))
		)
		if _, err := buffer.Write(buf[0:len]); err != nil {
			return err
		}
		if _, err := buffer.Write([]byte(str)); err != nil {
			return err
		}
		return nil
	}

	switch {
	case
		strings.HasPrefix(columnType, "Int"),
		strings.HasPrefix(columnType, "UInt"):
		value, ok := v.(int64)
		if !ok {
			return fmt.Errorf("unexpected type %T", v)
		}
		switch columnType {
		case "Int8":
			if err := binary.Write(buffer, binary.LittleEndian, int8(value)); err != nil {
				return err
			}
		case "Int16":
			if err := binary.Write(buffer, binary.LittleEndian, int16(value)); err != nil {
				return err
			}
		case "Int32":
			if err := binary.Write(buffer, binary.LittleEndian, int32(value)); err != nil {
				return err
			}
		case "Int64":
			if err := binary.Write(buffer, binary.LittleEndian, int64(value)); err != nil {
				return err
			}
		case "UInt8":
			if err := binary.Write(buffer, binary.LittleEndian, uint8(value)); err != nil {
				return err
			}
		case "UInt16":
			if err := binary.Write(buffer, binary.LittleEndian, uint16(value)); err != nil {
				return err
			}
		case "UInt32":
			if err := binary.Write(buffer, binary.LittleEndian, uint32(value)); err != nil {
				return err
			}
		case "UInt64":
			if err := binary.Write(buffer, binary.LittleEndian, uint64(value)); err != nil {
				return err
			}
		}
	case strings.HasPrefix(columnType, "Float"):
		value, ok := v.(float64)
		if !ok {
			return fmt.Errorf("unexpected type %T", v)
		}
		switch columnType {
		case "Float32":
			if err := binary.Write(buffer, binary.LittleEndian, math.Float32bits(float32(value))); err != nil {
				return err
			}
		case "Float64":
			if err := binary.Write(buffer, binary.LittleEndian, math.Float64bits(float64(value))); err != nil {
				return err
			}
		}
	case strings.HasPrefix(columnType, "FixedString"):
		var (
			strlen int
			str    []byte
		)
		switch v := v.(type) {
		case []byte:
			str = v
		case string:
			str = []byte(v)
		default:
			return fmt.Errorf("unexpected type %T", v)
		}
		if _, err := fmt.Sscanf(columnType, "FixedString(%d)", &strlen); err != nil {
			return err
		}
		if len(str) > strlen {
			return fmt.Errorf("too large value")
		}
		fixedString := make([]byte, strlen)
		copy(fixedString, str)
		if _, err := buffer.Write(fixedString); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unexpected type %T", v)
	}
	return nil
}
