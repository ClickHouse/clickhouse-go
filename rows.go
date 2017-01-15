package clickhouse

import (
	"bufio"
	"database/sql/driver"
	"io"
)

type rows struct {
	types   []string
	columns []string
	scanner *bufio.Scanner
}

func (rows *rows) Columns() []string {
	return rows.columns
}

func (rows *rows) Next(dest []driver.Value) error {
	return nil
}

func (rows *rows) Close() error {
	for {
		switch err := rows.Next(nil); err {
		case nil, io.EOF:
			return nil
		default:
			return err
		}
	}
}
