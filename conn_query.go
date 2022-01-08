package clickhouse

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/driver"
	"github.com/ClickHouse/clickhouse-go/lib/proto"
)

func (c *connect) query(ctx context.Context, query string, args ...interface{}) (*rows, error) {
	var (
		options   = queryOptions(ctx)
		body, err = bind(query, args...)
	)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}
	if c.err = c.sendQuery(body, &options); c.err != nil {
		return nil, c.err
	}
	init, err := c.nextBlock(&onProcess{})
	if err != nil {
		return nil, err
	}
	return &rows{
		conn: c,
		next: func() (*proto.Block, error) {
			return c.nextBlock(&onProcess{})
		},
		block:   init,
		columns: init.ColumnsNames(),
	}, nil
	//return ch.Rows(init, stream, errors), nil
}

type rows struct {
	err     error
	row     int
	next    func() (*proto.Block, error)
	conn    *connect
	block   *proto.Block
	columns []string
}

func (r *rows) Next() bool {
	if r.row >= r.block.Rows() {
		block, err := r.next()
		if err != nil {
			if err != io.EOF {
				r.err = err
			}
			return false
		}
		if block == nil || block.Rows() == 0 {
			return false
		}
		r.row, r.block = 0, block
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

func (r *rows) Err() error {
	return r.err
}

func (c *connect) queryBlock(ctx context.Context, query string, cb func(driver.Block), args ...interface{}) error {
	var (
		options   = queryOptions(ctx)
		body, err = bind(query, args...)
	)
	if err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}
	if err := c.sendQuery(body, &options); err != nil {
		return err
	}
	return c.process(&onProcess{
		data: func(b *proto.Block) {
			cb(b)
		},
	})
}
