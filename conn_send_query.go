package clickhouse

import (
	"github.com/ClickHouse/clickhouse-go/lib/proto"
)

// Connection::sendQuery
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Client/Connection.cpp
func (c *connect) sendQuery(body string, o *QueryOptions) error {
	c.debugf("[send query] compression=%t %s", c.compression, body)
	if err := c.encoder.Byte(proto.ClientQuery); err != nil {
		return err
	}
	q := proto.Query{
		ID:             o.queryID,
		Body:           body,
		QuotaKey:       o.quotaKey,
		Compression:    c.compression,
		InitialAddress: c.conn.LocalAddr().String(),
		Settings:       o.Settings(),
	}
	if err := q.Encode(c.encoder, c.revision); err != nil {
		return err
	}
	if err := c.sendData(&proto.Block{}, ""); err != nil {
		return err
	}
	return c.encoder.Flush()
}
