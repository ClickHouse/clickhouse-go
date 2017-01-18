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

type batch struct {
	numRows    uint
	values     []bytes.Buffer
	datapacket *datapacket
}

func (batch *batch) sendData(conn *connect) error {
	return batch.datapacket.sendData(conn, batch.numRows, batch.values)
}

func (batch *batch) insert(args []driver.Value) error {
	batch.numRows++
	if len(batch.values) == 0 && len(args) != 0 {
		batch.values = make([]bytes.Buffer, batch.datapacket.numColumns)
	}
	for i := 0; i < int(batch.datapacket.numColumns); i++ {
		if err := batch.write(i, args[i]); err != nil {
			return err
		}
	}
	return nil
}

func (batch *batch) write(i int, v driver.Value) error {
	var (
		column     = batch.datapacket.columns[i]
		columnType = batch.datapacket.columnsTypes[i]
	)

	switch columnType {
	case "Date", "DateTime":
		date, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("unexpected type %T for column %s (%s)", v, column, columnType)
		}
		if columnType == "Date" {
			if err := binary.Write(&batch.values[i], binary.LittleEndian, int16(date.Unix()/24/3600)); err != nil {
				return err
			}
		} else if err := binary.Write(&batch.values[i], binary.LittleEndian, int32(date.Unix())); err != nil {
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
		if _, err := batch.values[i].Write(buf[0:len]); err != nil {
			return err
		}
		if _, err := batch.values[i].WriteString(str); err != nil {
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
			if err := binary.Write(&batch.values[i], binary.LittleEndian, int8(value)); err != nil {
				return err
			}
		case "Int16":
			if err := binary.Write(&batch.values[i], binary.LittleEndian, int16(value)); err != nil {
				return err
			}
		case "Int32":
			if err := binary.Write(&batch.values[i], binary.LittleEndian, int32(value)); err != nil {
				return err
			}
		case "Int64":
			if err := binary.Write(&batch.values[i], binary.LittleEndian, int64(value)); err != nil {
				return err
			}
		case "UInt8":
			if err := binary.Write(&batch.values[i], binary.LittleEndian, uint8(value)); err != nil {
				return err
			}
		case "UInt16":
			if err := binary.Write(&batch.values[i], binary.LittleEndian, uint16(value)); err != nil {
				return err
			}
		case "UInt32":
			if err := binary.Write(&batch.values[i], binary.LittleEndian, uint32(value)); err != nil {
				return err
			}
		case "UInt64":
			if err := binary.Write(&batch.values[i], binary.LittleEndian, uint64(value)); err != nil {
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
			if err := binary.Write(&batch.values[i], binary.LittleEndian, math.Float32bits(float32(value))); err != nil {
				return err
			}
		case "Float64":
			if err := binary.Write(&batch.values[i], binary.LittleEndian, math.Float64bits(float64(value))); err != nil {
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
		if _, err := batch.values[i].Write(str); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unexpected type %T for column %s (%s)", v, column, columnType)
	}
	return nil
}
