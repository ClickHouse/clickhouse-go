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
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func (c *connect) query(ctx context.Context, query string, args ...interface{}) (*rows, error) {
	var (
		options   = queryOptions(ctx)
		onProcess = options.onProcess()
		body, err = bind(c.server.Timezone, query, args...)
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

	init, err := c.firstBlock(ctx, onProcess)

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
		err := c.process(ctx, onProcess)
		if err != nil {
			errors <- err
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
