package clickhouse

import (
	"fmt"

	"github.com/kshvakov/clickhouse/lib/data"
	"github.com/kshvakov/clickhouse/lib/protocol"
)

func (ch *clickhouse) readMeta() (*data.Block, error) {
	packet, err := ch.decoder.Uvarint()
	if err != nil {
		return nil, err
	}
	switch packet {
	case protocol.ServerData:
		block, err := ch.readBlock(ch.decoder)
		if err != nil {
			return nil, err
		}
		ch.logf("[read meta] <- data: packet=%d, columns=%d, rows=%d", packet, block.NumColumns, block.NumRows)
		return block, nil
	case protocol.ServerException:
		ch.logf("[read meta] <- exception")
		return nil, ch.exception(ch.decoder)
	default:
		return nil, fmt.Errorf("[read meta] unexpected packet [%d] from server", packet)
	}
}
