package clickhouse

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/driver"
	"github.com/ClickHouse/clickhouse-go/lib/driver/ch"
	"github.com/ClickHouse/clickhouse-go/lib/proto"
)

func (c *connect) query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
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
	init, err := c.processFirstBlock()
	if err != nil {
		return nil, err
	}
	var (
		errors = make(chan error)
		stream = make(chan *proto.Block)
	)
	go func() {
		c.err = c.process(&onProcess{
			data: func(b *proto.Block) {
				stream <- b
			},
		})
		if c.err != nil {
			errors <- c.err
		}
		close(errors)
		close(stream)
	}()

	return ch.Rows(init, stream, errors), nil
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
