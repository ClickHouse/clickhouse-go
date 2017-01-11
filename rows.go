package clickhouse

import (
	"bufio"
	"bytes"
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
	if !rows.scanner.Scan() {
		return io.EOF
	}
	values := bytes.Split(rows.scanner.Bytes(), []byte("\t"))
	for i := range dest {
		v, err := decode(rows.types[i], values[i])
		if err != nil {
			return err
		}
		dest[i] = v
	}
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
