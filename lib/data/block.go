package data

import (
	"database/sql/driver"
	"fmt"
	"io"
	"strings"

	"github.com/kshvakov/clickhouse/lib/binary"
	"github.com/kshvakov/clickhouse/lib/column"
	"github.com/kshvakov/clickhouse/lib/protocol"
	wb "github.com/kshvakov/clickhouse/lib/writebuffer"
)

type Block struct {
	Values     [][]interface{}
	Columns    []column.Column
	Buffers    []*buffer
	NumRows    uint64
	NumColumns uint64
	offsets    []uint64
	info       blockInfo
}

func (block *Block) ColumnNames() []string {
	names := make([]string, 0, len(block.Columns))
	for _, column := range block.Columns {
		names = append(names, column.Name())
	}
	return names
}

func (block *Block) Read(serverInfo *ServerInfo, decoder *binary.Decoder) (err error) {
	if serverInfo.Revision >= protocol.DBMS_MIN_REVISION_WITH_BLOCK_INFO {
		if err = block.info.read(decoder); err != nil {
			return err
		}
	}
	if block.NumColumns, err = decoder.Uvarint(); err != nil {
		return err
	}
	if block.NumRows, err = decoder.Uvarint(); err != nil {
		return err
	}
	block.Values = make([][]interface{}, block.NumColumns)
	for i := 0; i < int(block.NumColumns); i++ {
		var (
			value      interface{}
			columnName string
			columnType string
		)
		if columnName, err = decoder.String(); err != nil {
			return err
		}
		if columnType, err = decoder.String(); err != nil {
			return err
		}
		c, err := column.Factory(columnName, columnType, serverInfo.Timezone)
		if err != nil {
			return err
		}
		block.Columns = append(block.Columns, c)
		switch column := c.(type) {
		case *column.Array:
			offsets := make([]uint64, 0, block.NumRows)
			for row := 0; row < int(block.NumRows); row++ {
				offset, err := decoder.UInt64()
				if err != nil {
					return err
				}
				offsets = append(offsets, offset)
			}
			for n, offset := range offsets {
				ln := offset
				if n != 0 {
					ln = ln - offsets[n-1]
				}
				if value, err = column.ReadArray(decoder, int(ln)); err != nil {
					return err
				}
				block.Values[i] = append(block.Values[i], value)
			}
		default:
			for row := 0; row < int(block.NumRows); row++ {
				if value, err = column.Read(decoder); err != nil {
					return err
				}
				block.Values[i] = append(block.Values[i], value)
			}
		}
	}
	return nil
}

func (block *Block) AppendRow(args []driver.NamedValue) error {
	if len(block.Columns) != len(args) {
		return fmt.Errorf("block: expected %d arguments (columns: %s), got %d", len(block.Columns), strings.Join(block.ColumnNames(), ", "), len(args))
	}
	block.Reserve()
	{
		block.NumRows++
	}
	for num, c := range block.Columns {
		switch column := c.(type) {
		case *column.Array:
			ln, err := column.WriteArray(block.Buffers[num].Column, args[num].Value)
			if err != nil {
				return err
			}
			block.offsets[num] += ln
			if err := block.Buffers[num].Offset.UInt64(block.offsets[num]); err != nil {
				return err
			}
		default:
			if err := column.Write(block.Buffers[num].Column, args[num].Value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (block *Block) Reserve() {
	if len(block.Buffers) == 0 {
		block.Buffers = make([]*buffer, len(block.Columns))
		block.offsets = make([]uint64, len(block.Columns))
		for i := 0; i < len(block.Columns); i++ {
			var (
				offsetBuffer = wb.New(wb.InitialSize)
				columnBuffer = wb.New(wb.InitialSize)
			)
			block.Buffers[i] = &buffer{
				Offset:       binary.NewEncoder(offsetBuffer),
				Column:       binary.NewEncoder(columnBuffer),
				offsetBuffer: offsetBuffer,
				columnBuffer: columnBuffer,
			}
		}
	}
}

func (block *Block) Reset() {
	block.NumRows = 0
	block.NumColumns = 0
	for _, buffer := range block.Buffers {
		buffer.reset()
	}
	block.offsets = nil
	block.Buffers = nil
}

func (block *Block) Write(serverInfo *ServerInfo, encoder *binary.Encoder) error {
	defer block.Reset()
	if serverInfo.Revision >= protocol.DBMS_MIN_REVISION_WITH_BLOCK_INFO {
		if err := block.info.write(encoder); err != nil {
			return err
		}
	}
	encoder.Uvarint(block.NumColumns)
	encoder.Uvarint(block.NumRows)
	for i, column := range block.Columns {
		encoder.String(column.Name())
		encoder.String(column.CHType())
		if len(block.Buffers) == len(block.Columns) {
			if _, err := block.Buffers[i].WriteTo(encoder); err != nil {
				return err
			}
		}
	}
	return nil
}

type blockInfo struct {
	num1        uint64
	isOverflows bool
	num2        uint64
	bucketNum   int32
	num3        uint64
}

func (info *blockInfo) read(decoder *binary.Decoder) error {
	var err error
	if info.num1, err = decoder.Uvarint(); err != nil {
		return err
	}
	if info.isOverflows, err = decoder.Bool(); err != nil {
		return err
	}
	if info.num2, err = decoder.Uvarint(); err != nil {
		return err
	}
	if info.bucketNum, err = decoder.Int32(); err != nil {
		return err
	}
	if info.num3, err = decoder.Uvarint(); err != nil {
		return err
	}
	return nil
}

func (info *blockInfo) write(encoder *binary.Encoder) error {
	if err := encoder.Uvarint(1); err != nil {
		return err
	}
	if err := encoder.Bool(info.isOverflows); err != nil {
		return err
	}
	if err := encoder.Uvarint(2); err != nil {
		return err
	}
	if err := encoder.Int32(info.bucketNum); err != nil {
		return err
	}
	if err := encoder.Uvarint(0); err != nil {
		return err
	}
	return nil
}

type buffer struct {
	Offset       *binary.Encoder
	Column       *binary.Encoder
	offsetBuffer *wb.WriteBuffer
	columnBuffer *wb.WriteBuffer
}

func (buf *buffer) WriteTo(w io.Writer) (int64, error) {
	var size int64
	{
		ln, err := buf.offsetBuffer.WriteTo(w)
		if err != nil {
			return size, err
		}
		size += ln
	}
	{
		ln, err := buf.columnBuffer.WriteTo(w)
		if err != nil {
			return size, err
		}
		size += ln
	}
	return size, nil
}

func (buf *buffer) reset() {
	buf.offsetBuffer.Free()
	buf.columnBuffer.Free()
}
