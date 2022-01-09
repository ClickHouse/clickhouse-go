package clickhouse

import (
	"fmt"

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
	columns := r.block.Columns
	if len(columns) != len(dest) {
		return fmt.Errorf("sql: expected %d destination arguments in Scan, not %d", len(columns), len(dest))
	}
	for i, d := range dest {
		if err := columns[i].ScanRow(d, r.row-1); err != nil {
			return err
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
