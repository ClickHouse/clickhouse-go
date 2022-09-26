// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package clickhouse

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

var splitInsertRe = regexp.MustCompile(`(?i)\sVALUES\s*\(`)

func (c *connect) prepareBatch(ctx context.Context, query string, release func(*connect, error)) (driver.Batch, error) {
	query = splitInsertRe.Split(query, -1)[0]
	if !strings.HasSuffix(strings.TrimSpace(strings.ToUpper(query)), "VALUES") {
		query += " VALUES"
	}
	options := queryOptions(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}
	if err := c.sendQuery(query, &options); err != nil {
		release(c, err)
		return nil, err
	}
	var (
		onProcess  = options.onProcess()
		block, err = c.firstBlock(ctx, onProcess)
	)
	if err != nil {
		release(c, err)
		return nil, err
	}
	return &batch{
		ctx:   ctx,
		conn:  c,
		block: block,
		release: func(err error) {
			release(c, err)
		},
		onProcess: onProcess,
	}, nil
}

type batch struct {
	err       error
	ctx       context.Context
	conn      *connect
	sent      bool
	block     *proto.Block
	release   func(error)
	onProcess *onProcess
}

func (b *batch) Abort() error {
	defer func() {
		b.sent = true
		b.release(os.ErrProcessDone)
	}()
	if b.sent {
		return ErrBatchAlreadySent
	}
	return nil
}

func (b *batch) Append(v ...interface{}) error {
	if b.sent {
		return ErrBatchAlreadySent
	}
	if err := b.block.Append(v...); err != nil {
		b.release(err)
		return err
	}
	return nil
}

func (b *batch) AppendStruct(v interface{}) error {
	values, err := b.conn.structMap.Map("AppendStruct", b.block.ColumnsNames(), v, false)
	if err != nil {
		return err
	}
	return b.Append(values...)
}

func (b *batch) IsSent() bool {
	return b.sent
}

func (b *batch) Column(idx int) driver.BatchColumn {
	if len(b.block.Columns) <= idx {
		b.release(nil)
		return &batchColumn{
			err: &OpError{
				Op:  "batch.Column",
				Err: fmt.Errorf("invalid column index %d", idx),
			},
		}
	}
	return &batchColumn{
		batch:  b,
		column: b.block.Columns[idx],
		release: func(err error) {
			b.err = err
			b.release(err)
		},
	}
}

func (b *batch) Send() (err error) {
	defer func() {
		b.sent = true
		b.release(err)
	}()
	if b.sent {
		return ErrBatchAlreadySent
	}
	if b.err != nil {
		return b.err
	}
	if b.block.Rows() != 0 {
		if err = b.conn.sendData(b.block, ""); err != nil {
			return err
		}
	}
	if err = b.conn.sendData(&proto.Block{}, ""); err != nil {
		return err
	}
	if err = b.conn.process(b.ctx, b.onProcess); err != nil {
		return err
	}
	return nil
}

func (b *batch) Flush() error {
	if b.sent {
		return ErrBatchAlreadySent
	}
	if b.err != nil {
		return b.err
	}
	if b.block.Rows() != 0 {
		if err := b.conn.sendData(b.block, ""); err != nil {
			return err
		}
	}
	b.block.Reset()
	return nil
}

type batchColumn struct {
	err     error
	batch   driver.Batch
	column  column.Interface
	release func(error)
}

func (b *batchColumn) Append(v interface{}) (err error) {
	if b.batch.IsSent() {
		return ErrBatchAlreadySent
	}
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

var (
	_ (driver.Batch)       = (*batch)(nil)
	_ (driver.BatchColumn) = (*batchColumn)(nil)
)
