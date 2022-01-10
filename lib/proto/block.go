package proto

import (
	"database/sql/driver"
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

func (b *Block) AddColumn(name string, ct column.Type) error {
	column, err := ct.Column()
	if err != nil {
		return err
	}
	b.names = append(b.names, name)
	b.types = append(b.types, string(ct))
	b.Columns = append(b.Columns, column)
	return nil
}

func (b *Block) Append(v ...interface{}) (err error) {
	columns := b.Columns
	if len(columns) != len(v) {
		return &UnexpectedArguments{
			got:  len(v),
			want: len(columns),
		}
	}
	for i, v := range v {
		value := v
		if fn, ok := v.(driver.Valuer); ok {
			if value, err = fn.Value(); err != nil {
				return err
			}
		}
		if err := b.Columns[i].AppendRow(value); err != nil {
			return err
		}
	}
	return nil
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
				return &InvalidBlockData{
					msg: "mismatched len of columns",
				}
			}
		}
	}
	if err := encoder.Uvarint(uint64(len(b.Columns))); err != nil {
		return err
	}
	if err := encoder.Uvarint(b.rows); err != nil {
		return err
	}
	for i, c := range b.Columns {
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
	if b.rows > 1_000_000 {
		return &InvalidBlockData{
			msg: "more then 1 000 000 rows in block",
		}
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
		if err := encoder.Uvarint(1); err != nil {
			return err
		}
		if err := encoder.Bool(false); err != nil {
			return err
		}
		if err := encoder.Uvarint(2); err != nil {
			return err
		}
		if err := encoder.Int32(-1); err != nil {
			return err
		}
	}
	return encoder.Uvarint(0)
}

func decodeBlockInfo(decoder *binary.Decoder) error {
	{
		if _, err := decoder.Uvarint(); err != nil {
			return err
		}
		if _, err := decoder.Bool(); err != nil {
			return err
		}
		if _, err := decoder.Uvarint(); err != nil {
			return err
		}
		if _, err := decoder.Int32(); err != nil {
			return err
		}
	}
	if _, err := decoder.Uvarint(); err != nil {
		return err
	}
	return nil
}

type UnexpectedArguments struct{ got, want int }

func (e *UnexpectedArguments) Error() string {
	return fmt.Sprintf("clickhouse: expected %d arguments, got %d", e.want, e.got)
}

type InvalidBlockData struct {
	msg string
}

func (e *InvalidBlockData) Error() string {
	if len(e.msg) != 0 {
		return "clickhouse: invalid block data. " + e.msg
	}
	return "clickhouse: invalid block data"
}
