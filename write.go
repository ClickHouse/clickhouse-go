package clickhouse

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"
)

func writeBool(w io.Writer, v bool) error {
	value := []byte{0}
	if v {
		value[0] = 1
	}
	if _, err := w.Write(value); err != nil {
		return err
	}
	return nil
}

func writeUvarint(w io.Writer, v uint64) error {
	var (
		buf = make([]byte, binary.MaxVarintLen64)
		len = binary.PutUvarint(buf, v)
	)
	if _, err := w.Write(buf[0:len]); err != nil {
		return err
	}
	return nil
}

func writeString(w io.Writer, str string) error {
	if err := writeUvarint(w, uint64(len([]byte(str)))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(str)); err != nil {
		return err
	}
	return nil
}

func writeInt32(w io.Writer, v int32) error {
	chunk := make([]byte, 4)
	binary.LittleEndian.PutUint32(chunk, uint32(v))
	if _, err := w.Write(chunk); err != nil {
		return err
	}
	return nil
}

func writeUInt64(w io.Writer, v uint64) error {
	chunk := make([]byte, 8)
	binary.LittleEndian.PutUint64(chunk, v)
	if _, err := w.Write(chunk); err != nil {
		return err
	}
	return nil
}

func write(buffer *writeBuffer, columnInfo interface{}, v driver.Value) error {
	var err error
	switch columnInfo.(type) {
	case Date:
		var tv time.Time
		switch value := v.(type) {
		case time.Time:
			tv = value
		case string:
			if tv, err = time.Parse("2006-01-02", value); err != nil {
				return err
			}
			tv = time.Date(
				time.Time(tv).Year(),
				time.Time(tv).Month(),
				time.Time(tv).Day(),
				0, 0, 0, 0, time.UTC,
			)
		default:
			return fmt.Errorf("unexpected type %T", v)
		}
		binary.LittleEndian.PutUint16(buffer.alloc(2), uint16(tv.Unix()/24/3600))
	case DateTime:
		var tv time.Time
		switch value := v.(type) {
		case time.Time:
			tv = value
		case string:
			if tv, err = time.Parse("2006-01-02 15:04:05", value); err != nil {
				return err
			}
			tv = time.Date(
				time.Time(tv).Year(),
				time.Time(tv).Month(),
				time.Time(tv).Day(),
				time.Time(tv).Hour(),
				time.Time(tv).Minute(),
				time.Time(tv).Second(),
				0,
				time.UTC,
			)
		default:
			return fmt.Errorf("unexpected type %T", v)
		}
		binary.LittleEndian.PutUint32(buffer.alloc(4), uint32(tv.Unix()))
	case string:
		switch v := v.(type) {
		case []byte:
			var (
				scratch = make([]byte, binary.MaxVarintLen64)
				vlen    = binary.PutUvarint(scratch, uint64(len(v)))
			)
			if _, err := buffer.Write(scratch[:vlen]); err != nil {
				return err
			}
			if _, err := buffer.Write(v); err != nil {
				return err
			}
		case string:
			scratch := make([]byte, binary.MaxVarintLen64)
			vlen := binary.PutUvarint(scratch, uint64(len(v)))
			if _, err := buffer.Write(scratch[:vlen]); err != nil {
				return err
			}
			if _, err := buffer.Write([]byte(v)); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected type %T", v)
		}
	case []byte:
		var (
			str    []byte
			err    error
			strlen = len(columnInfo.([]byte))
		)
		switch v := v.(type) {
		case UUID:
			if str, err = uuid2bytes(string(v)); err != nil {
				return err
			}
		case []byte:
			str = v
		case string:
			str = []byte(v)
		default:
			return fmt.Errorf("unexpected type %T", v)
		}
		if len(str) > strlen {
			return fmt.Errorf("too large value (expected %d, got %d)", strlen, len(str))
		} else if len(str) == 0 {
			// When empty, insert default value to avoid allocation
			str = columnInfo.([]byte)
		} else if len(str) < strlen {
			fixedString := make([]byte, strlen)
			copy(fixedString, str)
			str = fixedString
		}
		if _, err := buffer.Write(str); err != nil {
			return err
		}
	case float32:
		buf := buffer.alloc(4)
		switch value := v.(type) {
		case float32:
			binary.LittleEndian.PutUint32(buf, math.Float32bits(value))
		case float64: // Implicit driver.Value coercion
			binary.LittleEndian.PutUint32(buf, math.Float32bits(float32(value)))
		default:
			return fmt.Errorf("unexpected type %T", v)
		}
	case float64:
		buf := buffer.alloc(8)
		switch value := v.(type) {
		case float64:
			binary.LittleEndian.PutUint64(buf, math.Float64bits(value))
		default:
			return fmt.Errorf("unexpected type %T", v)
		}
	case int8, uint8, enum8:
		buf := buffer.alloc(1)
		switch value := v.(type) {
		case int8:
			buf[0] = uint8(value)
		case uint8:
			buf[0] = value
		case int64: // Implicit driver.Value coercion
			buf[0] = uint8(value)
		default:
			return fmt.Errorf("unexpected type %T", v)
		}
	case int16, uint16, enum16:
		buf := buffer.alloc(2)
		switch value := v.(type) {
		case int16:
			binary.LittleEndian.PutUint16(buf, uint16(value))
		case uint16:
			binary.LittleEndian.PutUint16(buf, value)
		case int64: // Implicit driver.Value coercion
			binary.LittleEndian.PutUint16(buf, uint16(value))
		default:
			return fmt.Errorf("unexpected type %T", v)
		}
	case int32, uint32:
		buf := buffer.alloc(4)
		switch value := v.(type) {
		case int32:
			binary.LittleEndian.PutUint32(buf, uint32(value))
		case uint32:
			binary.LittleEndian.PutUint32(buf, value)
		case int64: // Implicit driver.Value coercion
			binary.LittleEndian.PutUint32(buf, uint32(value))
		default:
			return fmt.Errorf("unexpected type %T", v)
		}
	case int64, uint64:
		buf := buffer.alloc(8)
		switch value := v.(type) {
		case int64:
			binary.LittleEndian.PutUint64(buf, uint64(value))
		case uint64:
			binary.LittleEndian.PutUint64(buf, value)
		case []byte: // high bit uint64 hack
			copy(buf, value)
		default:
			return fmt.Errorf("unexpected type %T", v)
		}
	default:
		return fmt.Errorf("func write: unhandled type %T", columnInfo)
	}
	return nil
}
