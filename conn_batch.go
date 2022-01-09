package clickhouse

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/driver"
	"github.com/ClickHouse/clickhouse-go/lib/proto"
)

var (
	splitInsertRe = regexp.MustCompile(`(?i)\sVALUES\s*\(`)
)

// ch.sendQuery(ctx, splitInsertRe.Split(query, -1)[0]+" VALUES "
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
	block, err := c.firstBlock(&onProcess{})
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
		return &UnexpectedArguments{
			got:  len(v),
			want: len(columns),
		}
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
