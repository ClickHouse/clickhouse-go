package clickhouse

import (
	"github.com/kshvakov/clickhouse/internal/data"
	"github.com/kshvakov/clickhouse/internal/protocol"
)

func (ch *clickhouse) readBlock() (*data.Block, error) {
	if ch.ServerInfo.Revision >= protocol.DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
		if _, err := ch.decoder.String(); err != nil {
			return nil, err
		}
	}
	if ch.compress {

	}
	var block data.Block
	if err := block.Read(&ch.ServerInfo, ch.decoder); err != nil {
		return nil, err
	}
	return &block, nil
}
