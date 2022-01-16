package clickhouse

import (
	"database/sql"
	"io"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type rows struct {
	err     error
	row     int
	conn    *connect
	block   *proto.Block
	totals  *proto.Block
	errors  chan error
	stream  chan *proto.Block
	columns []string
}

func (r *rows) Next() (result bool) {
	defer func() {
		if !result {
			r.Close()
		}
	}()
	if r.block == nil {
		return false
	}
next:
	if r.row >= r.block.Rows() {
		select {
		case err := <-r.errors:
			if err != nil {
				r.err, r.conn.err = err, err
				return false
			}
			goto next
		case block := <-r.stream:
			if block == nil || block.Rows() == 0 {
				return false
			}
			if block.Packet == proto.ServerTotals {
				r.row, r.block, r.totals = 0, nil, block
				return false
			}
			r.row, r.block = 0, block
		}
	}
	r.row++
	return true
}

func (r *rows) Scan(dest ...interface{}) error {
	if r.block == nil || (r.row == 0 && r.row >= r.block.Rows()) { // call without next when result is empty
		return io.EOF
	}
	return scan(r.block, r.row, dest...)
}

func (r *rows) ScanStruct(dest interface{}) error {
	values, err := structToScannableValues(r.columns, dest)
	if err != nil {
		return err
	}
	return r.Scan(values...)
}

func (r *rows) Totals(dest ...interface{}) error {
	if r.totals == nil {
		return sql.ErrNoRows
	}
	return scan(r.totals, 1, dest...)
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

func (r *row) ScanStruct(dest interface{}) error {
	values, err := structToScannableValues(r.rows.columns, dest)
	if err != nil {
		return err
	}
	return r.Scan(values...)
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
