package clickhouse

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

var splitInsertRe = regexp.MustCompile(`(?i)\sVALUES\s*\(`)

func (c *connect) prepareBatch(ctx context.Context, query string, release func(*connect)) (*batch, error) {
	query = splitInsertRe.Split(query, -1)[0]
	if !strings.HasSuffix(strings.TrimSpace(strings.ToUpper(query)), "VALUES") {
		query += " VALUES"
	}
	options := queryOptions(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}
	if c.err = c.sendQuery(query, &options); c.err != nil {
		release(c)
		return nil, c.err
	}
	var (
		onProcess  = options.onProcess()
		block, err = c.firstBlock(onProcess)
	)
	if err != nil {
		release(c)
		return nil, err
	}
	return &batch{
		conn:  c,
		block: block,
		release: func(err error) {
			c.err = err
			release(c)
		},
		onProcess: onProcess,
	}, nil
}

type batch struct {
	err       error
	conn      *connect
	sent      bool
	block     *proto.Block
	release   func(error)
	onProcess *onProcess
}

func (b *batch) Append(v ...interface{}) error {
	if err := b.block.Append(v...); err != nil {
		b.release(err)
		return err
	}
	return nil
}

func (b *batch) Column(idx int) driver.BatchColumn {
	if len(b.block.Columns) <= idx {
		b.release(nil)
		return &batchColumn{
			err: &InvalidColumnIndex{
				op:  "batch.Column(idx)",
				idx: idx,
			},
		}
	}
	return &batchColumn{
		column: b.block.Columns[idx],
		release: func(err error) {
			b.err = err
			b.release(err)
		},
	}
}

func (b *batch) Send() (err error) {
	defer b.release(err)
	defer func() {
		b.sent = true
	}()
	if b.sent {
		return &BatchAlreadySent{}
	}
	if b.err != nil {
		return b.err
	}
	if err = b.conn.sendData(b.block, ""); err != nil {
		return err
	}
	if err = b.conn.sendData(&proto.Block{}, ""); err != nil {
		return err
	}
	if err = b.conn.encoder.Flush(); err != nil {
		return err
	}
	if err = b.conn.process(b.onProcess); err != nil {
		return err
	}
	return nil
}

type batchColumn struct {
	err     error
	column  column.Interface
	release func(error)
}

func (b *batchColumn) Append(v interface{}) (err error) {
	if b.err != nil {
		b.release(b.err)
		return b.err
	}
	if _, err = b.column.Append(v); err != nil {
		b.release(err)
		return err
	}
	return nil
}

var _ (driver.Batch) = (*batch)(nil)
var _ (driver.BatchColumn) = (*batchColumn)(nil)
