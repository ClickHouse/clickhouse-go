package clickhouse

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func (c *connect) query(ctx context.Context, query string, args ...interface{}) (*rows, error) {
	var (
		options   = queryOptions(ctx)
		onProcess = options.onProcess()
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

	init, err := c.firstBlock(onProcess)

	if err != nil {
		return nil, err
	}

	var (
		errors = make(chan error)
		stream = make(chan *proto.Block, 2)
	)

	go func() {
		onProcess.data = func(b *proto.Block) {
			stream <- b
		}
		c.err = c.process(onProcess)
		if c.err != nil {
			errors <- c.err
		}
		close(errors)
		close(stream)
	}()

	return &rows{
		conn:    c,
		block:   init,
		stream:  stream,
		errors:  errors,
		columns: init.ColumnsNames(),
	}, nil
}

func (c *connect) queryRow(ctx context.Context, query string, args ...interface{}) *row {
	rows, err := c.query(ctx, query, args...)
	if err != nil {
		return &row{
			err: err,
		}
	}
	return &row{
		rows: rows,
	}
}
