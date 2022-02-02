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
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// Connection::ping
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Client/Connection.cpp
func (c *connect) ping(ctx context.Context) error {
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}
	c.debugf("[ping] -> ping")
	if c.err = c.encoder.Byte(proto.ClientPing); c.err != nil {
		return c.err
	}
	if c.err = c.encoder.Flush(); c.err != nil {
		return c.err
	}
	var packet byte
	for {
		if packet, c.err = c.decoder.ReadByte(); c.err != nil {
			return c.err
		}
		switch packet {
		case proto.ServerProgress:
			if _, c.err = c.progress(); c.err != nil {
				return c.err
			}
		case proto.ServerPong:
			c.debugf("[ping] <- pong")
			return nil
		default:
			c.err = os.ErrInvalid
			return fmt.Errorf("unexpected packet %d", packet)
		}
	}
}
