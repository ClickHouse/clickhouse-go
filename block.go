package clickhouse

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
)

// data block
type block struct {
	table       string
	info        blockInfo
	numRows     uint64
	numColumns  uint64
	columnNames []string
	columnTypes []string
	columns     [][]interface{}
	buffers     []bytes.Buffer
}

type blockInfo struct {
	num1        uint64
	isOverflows bool
	num2        uint64
	bucketNum   int32
	num3        uint64
}

func (info *blockInfo) read(conn *connect) error {
	var err error
	if info.num1, err = readUvariant(conn); err != nil {
		return err
	}
	if info.isOverflows, err = readBool(conn); err != nil {
		return err
	}
	if info.num2, err = readUvariant(conn); err != nil {
		return err
	}
	if info.bucketNum, err = readInt32(conn); err != nil {
		return err
	}
	if info.num3, err = readUvariant(conn); err != nil {
		return err
	}
	return nil
}

func (info *blockInfo) write(conn *connect) error {
	if err := writeUvarint(conn, info.num1); err != nil {
		return err
	}
	if info.num1 != 0 {
		if err := writeBool(conn, info.isOverflows); err != nil {
			return err
		}
		if err := writeUvarint(conn, info.num2); err != nil {
			return err
		}
		if err := writeInt32(conn, info.bucketNum); err != nil {
			return err
		}
		if err := writeUvarint(conn, info.num3); err != nil {
			return err
		}
	}
	return nil
}

func (b *block) read(revision uint64, conn *connect) error {
	var err error
	if revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
		if b.table, err = readString(conn); err != nil {
			return err
		}
	}
	if revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
		if err := b.info.read(conn); err != nil {
			return err
		}
	}
	if b.numColumns, err = readUvariant(conn); err != nil {
		return err
	}
	if b.numRows, err = readUvariant(conn); err != nil {
		return err
	}
	b.columns = make([][]interface{}, b.numColumns)
	for i := 0; i < int(b.numColumns); i++ {
		var column, columnType string
		if column, err = readString(conn); err != nil {
			return err
		}
		if columnType, err = readString(conn); err != nil {
			return err
		}
		b.columnNames = append(b.columnNames, column)
		b.columnTypes = append(b.columnTypes, columnType)
		for row := 0; row < int(b.numRows); row++ {
			value, err := read(conn, columnType)
			if err != nil {
				return err
			}
			b.columns[i] = append(b.columns[i], value)
		}
	}
	return nil
}

func (b *block) write(revision uint64, conn *connect) error {
	if err := writeUvarint(conn, ClientDataPacket); err != nil {
		return err
	}
	if revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
		if err := writeString(conn, b.table); err != nil {
			return err
		}
	}
	if revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
		if err := b.info.write(conn); err != nil {
			return err
		}
	}
	if err := writeUvarint(conn, b.numColumns); err != nil {
		return err
	}
	if err := writeUvarint(conn, b.numRows); err != nil {
		return err
	}
	for i, column := range b.columnNames {
		columnType := b.columnTypes[i]
		if err := writeString(conn, column); err != nil {
			return err
		}
		if err := writeString(conn, columnType); err != nil {
			return err
		}
		if _, err := b.buffers[i].WriteTo(conn); err != nil {
			return err
		}
	}
	return nil
}

func (b *block) append(args []driver.Value) error {
	if len(b.buffers) == 0 && len(args) != 0 {
		b.numRows = 0
		b.buffers = make([]bytes.Buffer, len(args))
	}
	b.numRows++
	for columnNum := range b.columnTypes {
		if err := b.appendValue(columnNum, args[columnNum]); err != nil {
			return err
		}
	}
	return nil
}

func (b *block) appendValue(i int, v driver.Value) error {
	var (
		column     = b.columnNames[i]
		columnType = b.columnTypes[i]
		buffer     = &b.buffers[i]
	)

	switch columnType {
	case "Date", "DateTime":
		date, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("unexpected type %T for column %s (%s)", v, column, columnType)
		}
		if columnType == "Date" {
			if err := binary.Write(buffer, binary.LittleEndian, int16(date.Truncate(24*3600).Unix()/24/3600)+1); err != nil {
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
			return fmt.Errorf("unexpected type %T for column %s (%s)", v, column, columnType)
		}
		var (
			buf = make([]byte, binary.MaxVarintLen64)
			len = binary.PutUvarint(buf, uint64(len(str)))
		)
		if _, err := buffer.Write(buf[0:len]); err != nil {
			return err
		}
		if _, err := buffer.WriteString(str); err != nil {
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
			return fmt.Errorf("unexpected type %T for column %s (%s)", v, column, columnType)
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
			return fmt.Errorf("unexpected type %T for column %s (%s)", v, column, columnType)
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
			fmt.Errorf("unexpected type %T for column %s (%s)", v, column, columnType)
		}
		if _, err := fmt.Sscanf(columnType, "FixedString(%d)", &strlen); err != nil {
			return err
		}
		if len(str) > strlen {
			return fmt.Errorf("too large value for column %s (%s)", column, columnType)
		}
		if _, err := buffer.Write(str); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unexpected type %T for column %s (%s)", v, column, columnType)
	}
	return nil
}
