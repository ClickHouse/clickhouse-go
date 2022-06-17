// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package proto

import (
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
)

type Block struct {
	names   []string
	Packet  byte
	Columns []column.Interface
}

func (b *Block) Rows() int {
	if len(b.Columns) == 0 {
		return 0
	}
	return b.Columns[0].Rows()
}

func (b *Block) AddColumn(name string, ct column.Type) error {
	column, err := ct.Column(name)
	if err != nil {
		return err
	}
	b.names, b.Columns = append(b.names, name), append(b.Columns, column)
	return nil
}

func (b *Block) Append(v ...interface{}) (err error) {
	columns := b.Columns
	if len(columns) != len(v) {
		return &BlockError{
			Op:  "Append",
			Err: fmt.Errorf("clickhouse: expected %d arguments, got %d", len(columns), len(v)),
		}
	}
	for i, v := range v {
		if err := b.Columns[i].AppendRow(v); err != nil {
			return &BlockError{
				Op:         "AppendRow",
				Err:        err,
				ColumnName: columns[i].Name(),
			}
		}
	}
	return nil
}

func (b *Block) ColumnsNames() []string {
	return b.names
}

func (b *Block) Encode(encoder *binary.Encoder, revision uint64) error {
	if revision > 0 {
		if err := encodeBlockInfo(encoder); err != nil {
			return err
		}
	}
	var rows int
	if len(b.Columns) != 0 {
		rows = b.Columns[0].Rows()
		for _, c := range b.Columns[1:] {
			if rows != c.Rows() {
				return &BlockError{
					Op:  "Encode",
					Err: errors.New("mismatched len of columns"),
				}
			}
		}
	}
	if err := encoder.Uvarint(uint64(len(b.Columns))); err != nil {
		return err
	}
	if err := encoder.Uvarint(uint64(rows)); err != nil {
		return err
	}
	for _, c := range b.Columns {
		if err := encoder.String(c.Name()); err != nil {
			return err
		}
		if err := encoder.String(string(c.Type())); err != nil {
			return err
		}
		if serialize, ok := c.(column.CustomSerialization); ok {
			if err := serialize.WriteStatePrefix(encoder); err != nil {
				return &BlockError{
					Op:         "Encode",
					Err:        err,
					ColumnName: c.Name(),
				}
			}
		}
		if err := c.Encode(encoder); err != nil {
			return &BlockError{
				Op:         "Encode",
				Err:        err,
				ColumnName: c.Name(),
			}
		}
	}
	return nil
}

func (b *Block) Decode(decoder *binary.Decoder, revision uint64) (err error) {
	if revision > 0 {
		if err := decodeBlockInfo(decoder); err != nil {
			return err
		}
	}
	var (
		numRows uint64
		numCols uint64
	)
	if numCols, err = decoder.Uvarint(); err != nil {
		return err
	}
	if numRows, err = decoder.Uvarint(); err != nil {
		return err
	}
	if numRows > 1_000_000_000 {
		return &BlockError{
			Op:  "Decode",
			Err: errors.New("more then 1 billion rows in block"),
		}
	}
	b.Columns = make([]column.Interface, 0, numCols)
	for i := 0; i < int(numCols); i++ {
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
		c, err := column.Type(columnType).Column(columnName)
		if err != nil {
			return err
		}
		if numRows != 0 {
			if serialize, ok := c.(column.CustomSerialization); ok {
				if err := serialize.ReadStatePrefix(decoder); err != nil {
					return &BlockError{
						Op:         "Decode",
						Err:        err,
						ColumnName: columnName,
					}
				}
			}
			if err := c.Decode(decoder, int(numRows)); err != nil {
				return &BlockError{
					Op:         "Decode",
					Err:        err,
					ColumnName: columnName,
				}
			}
		}
		b.names, b.Columns = append(b.names, columnName), append(b.Columns, c)
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

type BlockError struct {
	Op         string
	Err        error
	ColumnName string
}

func (e *BlockError) Error() string {
	switch err := e.Err.(type) {
	case *column.Error:
		return fmt.Sprintf("clickhouse [%s]: (%s %s) %s", e.Op, e.ColumnName, err.ColumnType, err.Err)
	case *column.DateOverflowError:
		return fmt.Sprintf("clickhouse: dateTime overflow. %s must be between %s and %s", e.ColumnName, err.Min.Format(err.Format), err.Max.Format(err.Format))
	}
	return fmt.Sprintf("clickhouse [%s]: %s %s", e.Op, e.ColumnName, e.Err)
}
