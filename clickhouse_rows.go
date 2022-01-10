package clickhouse

import (
	"database/sql"
	"fmt"
	"io"

	"github.com/ClickHouse/clickhouse-go/lib/proto"
)

type rows struct {
	err     error
	row     int
	conn    *connect
	block   *proto.Block
	errors  chan error
	stream  chan *proto.Block
	columns []string
}

func (r *rows) Next() bool {
next:
	if r.row >= r.block.Rows() {
		select {
		case err := <-r.errors:
			if err != nil {
				r.err = err
				return true
			}
			goto next
		case block := <-r.stream:
			if block == nil || block.Rows() == 0 {
				return false
			}
			r.row, r.block = 0, block
		}
	}
	r.row++
	return true
}

func (r *rows) Scan(dest ...interface{}) error {
	if r.row == 0 && r.row >= r.block.Rows() { // call without next when result is empty
		return io.EOF
	}
	columns := r.block.Columns
	if len(columns) != len(dest) {
		return fmt.Errorf("sql: expected %d destination arguments in Scan, not %d", len(columns), len(dest))
	}
	for i, d := range dest {
		switch d := d.(type) {
		case sql.Scanner:
			if err := d.Scan(columns[i].RowValue(r.row - 1)); err != nil {
				return err
			}
		default:
			if err := columns[i].ScanRow(d, r.row-1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Close() error {
	for range r.stream {
	}
	for range r.errors {
	}
	return nil
}

func (r *rows) Err() error {
	return r.err
}

type row struct {
	err  error
	rows *rows
}

func (r *row) Err() error {
	return r.err
}

func (r *row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	err := r.rows.Scan(dest...)
	if err != nil {
		return err
	}
	return r.rows.Close()
}
