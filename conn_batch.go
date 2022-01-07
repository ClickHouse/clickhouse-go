package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/driver"
	"github.com/ClickHouse/clickhouse-go/lib/proto"
)

func (c *connect) prepareBatch(ctx context.Context, query string, release func(*connect)) (driver.Batch, error) {
	options := queryOptions(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}
	if c.err = c.sendQuery(query, &options); c.err != nil {
		release(c)
		return nil, c.err
	}
	block, err := c.processFirstBlock()
	if err != nil {
		release(c)
		return nil, err
	}
	return &batch{
		conn:  c,
		block: block,
		release: func(c *connect, err error) {
			c.err = err
			release(c)
		},
	}, nil
}

type batch struct {
	conn    *connect
	block   *proto.Block
	release func(*connect, error)
}

func (b *batch) Append(v ...interface{}) error {
	columns := b.block.Columns
	if len(columns) != len(v) {
		return fmt.Errorf("")
	}
	for i, v := range v {
		if err := b.block.Columns[i].AppendRow(v); err != nil {
			return err
		}
	}

	return nil
}
func (b *batch) Column(int) (driver.BatchColumn, error) {
	return &batchColumn{}, nil
}
func (b *batch) Send() (err error) {
	defer b.release(b.conn, err)
	if err = b.conn.sendData(b.block, ""); err != nil {
		return err
	}
	if err = b.conn.sendData(&proto.Block{}, ""); err != nil {
		return err
	}
	if err = b.conn.encoder.Flush(); err != nil {
		return err
	}
	if err = b.conn.process(&onProcess{}); err != nil {
		return err
	}
	return nil
}

type batchColumn struct {
}

func (b *batchColumn) Append(v interface{}) error {
	return nil
}

var _ (driver.Batch) = (*batch)(nil)
var _ (driver.BatchColumn) = (*batchColumn)(nil)
