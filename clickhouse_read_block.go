package clickhouse

import (
	"time"

	"github.com/kshvakov/clickhouse/lib/data"
)

func (ch *clickhouse) readBlock() (*data.Block, error) {
	{
		ch.conn.SetReadDeadline(time.Now().Add(ch.readTimeout))
		ch.conn.SetWriteDeadline(time.Now().Add(ch.writeTimeout))
	}

	if _, err := ch.decoder.String(); err != nil { // temporary table
		return nil, err
	}

	if ch.compress {

	}
	var block data.Block
	if err := block.Read(&ch.ServerInfo, ch.decoder); err != nil {
		return nil, err
	}
	return &block, nil
}
