package clickhouse

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/proto"
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
