package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
)

// data block
type block struct {
	table         string
	info          blockInfo
	numRows       uint64
	numColumns    uint64
	columnNames   []string
	columnTypes   []string
	columnInfo    []interface{}
	columns       [][]interface{}
	offsets       []uint64
	buffers       []*writeBuffer
	offsetBuffers []*writeBuffer
}

type blockInfo struct {
	num1        uint64
	isOverflows bool
	num2        uint64
	bucketNum   int32
	num3        uint64
}

func (info *blockInfo) read(r io.Reader) error {
	var err error
	if info.num1, err = readUvarint(r); err != nil {
		return err
	}
	if info.isOverflows, err = readBool(r); err != nil {
		return err
	}
	if info.num2, err = readUvarint(r); err != nil {
		return err
	}
	if info.bucketNum, err = readInt32(r); err != nil {
		return err
	}
	if info.num3, err = readUvarint(r); err != nil {
		return err
	}
	return nil
}

func (info *blockInfo) write(w io.Writer) error {
	if err := writeUvarint(w, info.num1); err != nil {
		return err
	}
	if info.num1 != 0 {
		if err := writeBool(w, info.isOverflows); err != nil {
			return err
		}
		if err := writeUvarint(w, info.num2); err != nil {
			return err
		}
		if err := writeInt32(w, info.bucketNum); err != nil {
			return err
		}
		if err := writeUvarint(w, info.num3); err != nil {
			return err
		}
	}
	return nil
}

func (b *block) read(revision uint64, r io.Reader) error {
	var err error
	if revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
		if b.table, err = readString(r); err != nil {
			return err
		}
	}
	if revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
		if err := b.info.read(r); err != nil {
			return err
		}
	}
	if b.numColumns, err = readUvarint(r); err != nil {
		return err
	}
	if b.numRows, err = readUvarint(r); err != nil {
		return err
	}
	b.columns = make([][]interface{}, b.numColumns)
	for i := 0; i < int(b.numColumns); i++ {
		var columnName, columnType string

		if columnName, err = readString(r); err != nil {
			return err
		}
		if columnType, err = readString(r); err != nil {
			return err
		}
		// Coerce column type to Go type
		columnInfo, err := toColumnType(columnType)
		if err != nil {
			return err
		}
		b.columnInfo = append(b.columnInfo, columnInfo)
		b.columnNames = append(b.columnNames, columnName)
		b.columnTypes = append(b.columnTypes, columnType)
		switch info := columnInfo.(type) {
		case array:
			offsets := make([]uint64, 0, b.numRows)
			for row := 0; row < int(b.numRows); row++ {
				offset, err := readUInt64(r)
				if err != nil {
					return err
				}
				offsets = append(offsets, offset)
			}
			for n, offset := range offsets {
				len := offset
				if n != 0 {
					len = len - offsets[n-1]
				}
				value, err := readArray(r, info.baseType, len)
				if err != nil {
					return err
				}
				b.columns[i] = append(b.columns[i], value)
			}
		default:
			for row := 0; row < int(b.numRows); row++ {
				value, err := read(r, columnInfo)
				if err != nil {
					return err
				}
				b.columns[i] = append(b.columns[i], value)
			}
		}
	}
	return nil
}

func (b *block) write(revision uint64, w io.Writer) error {
	if err := writeUvarint(w, ClientDataPacket); err != nil {
		return err
	}
	if revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
		if err := writeString(w, b.table); err != nil {
			return err
		}
	}
	if revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
		if err := b.info.write(w); err != nil {
			return err
		}
	}
	if err := writeUvarint(w, b.numColumns); err != nil {
		return err
	}
	if err := writeUvarint(w, b.numRows); err != nil {
		return err
	}
	b.numRows = 0
	b.offsets = make([]uint64, len(b.columnNames))
	for i, column := range b.columnNames {
		columnType := b.columnTypes[i]
		if err := writeString(w, column); err != nil {
			return err
		}
		if err := writeString(w, columnType); err != nil {
			return err
		}
		if err := b.offsetBuffers[i].writeTo(w); err != nil {
			return err
		}
		if err := b.buffers[i].writeTo(w); err != nil {
			return err
		}
	}
	return nil
}

func (b *block) reserveColumns() {
	if len(b.buffers) == 0 {
		columnCount := len(b.columnNames)
		b.numRows = 0
		b.offsets = make([]uint64, columnCount)
		b.buffers = make([]*writeBuffer, columnCount)
		b.offsetBuffers = make([]*writeBuffer, columnCount)
		for i := 0; i < columnCount; i++ {
			b.buffers[i] = wb(WriteBufferInitialSize)
			b.offsetBuffers[i] = wb(WriteBufferInitialSize)
		}
	}
}

func (b *block) append(args []driver.Value) error {
	if len(b.columnNames) != len(args) {
		return fmt.Errorf("block: expected %d arguments (columns: %s), got %d", len(b.columnNames), strings.Join(b.columnNames, ", "), len(args))
	}
	b.reserveColumns()
	b.numRows++
	for columnNum, info := range b.columnInfo {
		var (
			column = b.columnNames[columnNum]
			buffer = b.buffers[columnNum]
			offset = b.offsetBuffers[columnNum]
		)
		switch v := info.(type) {
		case array:
			switch tArray := args[columnNum].(type) {
			case *array:
				arrayLen, err := tArray.write(v.baseType, buffer)
				if err != nil {
					return fmt.Errorf("column %s (%s): %s", column, b.columnTypes[columnNum], err.Error())
				}
				b.offsets[columnNum] += arrayLen
				if err := writeUInt64(offset, b.offsets[columnNum]); err != nil {
					return err
				}
			case []byte:
				ct, arrayLen, data, err := arrayInfo(tArray)
				if err != nil {
					return err
				}
				b.offsets[columnNum] += arrayLen
				if err := writeUInt64(offset, b.offsets[columnNum]); err != nil {
					return err
				}
				switch v := v.baseType.(type) {
				case enum8:
					if data, err = arrayStringToArrayEnum(arrayLen, data, enum(v)); err != nil {
						return err
					}
				case enum16:
					if data, err = arrayStringToArrayEnum(arrayLen, data, enum(v)); err != nil {
						return err
					}
				default:
					if "Array("+ct+")" != b.columnTypes[columnNum] {
						return fmt.Errorf("column %s (%s): unexpected type %s of value", column, b.columnTypes[columnNum], ct)
					}
				}
				if _, err := buffer.Write(data); err != nil {
					return err
				}
			default:
				return fmt.Errorf("column %s (%s): unexpected type %T of value", column, b.columnTypes[columnNum], args[columnNum])
			}
		case enum8:
			var (
				value interface{} = args[columnNum]
				err   error
			)
			// If argument is string, resolve identifier to value
			ident, ok := args[columnNum].(string)
			if ok {
				value, err = enum(v).toValue(ident)
			}
			if err != nil {
				return fmt.Errorf("column %s (%s): %s", column, b.columnTypes[columnNum], err.Error())
			}
			if err := write(buffer, info, value); err != nil {
				return fmt.Errorf("column %s (%s): %s", column, b.columnTypes[columnNum], err.Error())
			}
		case enum16:
			var (
				value interface{} = args[columnNum]
				err   error
			)
			// If argument is string, resolve identifier to value
			ident, ok := args[columnNum].(string)
			if ok {
				value, err = enum(v).toValue(ident)
			}
			if err != nil {
				return fmt.Errorf("column %s (%s): %s", column, b.columnTypes[columnNum], err.Error())
			}
			if err := write(buffer, info, value); err != nil {
				return fmt.Errorf("column %s (%s): %s", column, b.columnTypes[columnNum], err.Error())
			}
		default:
			if err := write(buffer, info, args[columnNum]); err != nil {
				return fmt.Errorf("column %s (%s): %s", column, b.columnTypes[columnNum], err.Error())
			}
		}
	}
	return nil
}

// Reset and recycle column buffers
func (b *block) reset() {
	if b == nil {
		return
	}
	for _, b := range b.buffers {
		b.free()
	}
	for _, b := range b.offsetBuffers {
		b.free()
	}
	b.buffers = nil
	b.offsets = nil
}
