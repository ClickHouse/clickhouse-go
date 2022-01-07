package proto

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"github.com/ClickHouse/clickhouse-go/lib/column"
)

type Block struct {
	Columns    []column.Interface
	rows       uint64
	names      []string
	types      []string
	numColumns uint64
}

func (b *Block) Rows() int {
	return int(b.rows)
}

func (b *Block) ColumnsNames() []string {
	return b.names
}

func (b *Block) Encode(encoder *binary.Encoder, revision uint64) error {
	if err := encodeBlockInfo(encoder); err != nil {
		return err
	}
	if len(b.Columns) != 0 {
		b.rows = uint64(b.Columns[0].Rows())
		for _, c := range b.Columns[1:] {
			if b.rows != uint64(c.Rows()) {
				return fmt.Errorf("invalid data")
			}
		}
	}
	encoder.Uvarint(uint64(len(b.Columns)))
	encoder.Uvarint(b.rows)
	for i, c := range b.Columns {
		//	fmt.Println("ENCODE", b.names[i], b.types[i], b.rows, c)
		if err := encoder.String(b.names[i]); err != nil {
			return err
		}
		if err := encoder.String(b.types[i]); err != nil {
			return err
		}
		if err := c.Encode(encoder); err != nil {
			return err
		}
	}
	return nil
}

func (b *Block) Decode(decoder *binary.Decoder, revision uint64) (err error) {
	if err := decodeBlockInfo(decoder); err != nil {
		return err
	}
	if b.numColumns, err = decoder.Uvarint(); err != nil {
		return err
	}
	if b.rows, err = decoder.Uvarint(); err != nil {
		return err
	}
	if b.rows > 100000 {
		return fmt.Errorf("invalid block")
	}
	b.Columns = make([]column.Interface, 0, b.numColumns)
	for i := 0; i < int(b.numColumns); i++ {
		var (
			columnName string
			columnType string
		)
		if columnName, err = decoder.String(); err != nil {
			return err
		}
		if columnType, err = decoder.String(); err != nil {
			return err
		}
		column, err := column.Type(columnType).Column()
		if err != nil {
			return err
		}
		if b.rows != 0 {
			if err := column.Decode(decoder, int(b.rows)); err != nil {
				return err
			}
		}
		b.Columns = append(b.Columns, column)
		b.names, b.types = append(b.names, columnName), append(b.types, columnType)
	}
	return nil
}

func encodeBlockInfo(encoder *binary.Encoder) error {
	{
		encoder.Uvarint(1)
		encoder.Bool(false)
		encoder.Uvarint(2)
		encoder.Int32(-1)
	}
	return encoder.Uvarint(0)
}

func decodeBlockInfo(decoder *binary.Decoder) error {
	{
		decoder.Uvarint()
		decoder.Bool()
		decoder.Uvarint()
		decoder.Int32()
	}
	if _, err := decoder.Uvarint(); err != nil {
		return err
	}
	return nil
}
