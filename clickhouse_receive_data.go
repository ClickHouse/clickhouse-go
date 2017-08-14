package clickhouse

import (
	"fmt"

	"github.com/kshvakov/clickhouse/internal/data"
	"github.com/kshvakov/clickhouse/internal/protocol"
)

func (ch *clickhouse) receiveData(block *data.Block) error {
	for {
		packet, err := ch.decoder.Uvarint()
		if err != nil {
			return err
		}
		switch packet {
		case protocol.ServerException:
			ch.logf("[receive packet] <- exception")
			return ch.exception()
		case protocol.ServerProgress:
			progress, err := ch.progress()
			if err != nil {
				return err
			}
			ch.logf("[receive packet] <- progress: rows=%d, bytes=%d, total rows=%d",
				progress.bytes,
				progress.rows,
				progress.totalRows,
			)
		case
			protocol.ServerData,
			protocol.ServerTotals,
			protocol.ServerExtremes:
			if ch.ServerInfo.Revision >= protocol.DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
				if _, err := ch.decoder.String(); err != nil {
					return err
				}
			}
			if err := block.Read(&ch.ServerInfo, ch.decoder); err != nil {
				ch.logf("[receive packet] err: %v", err)
				return err
			}
			ch.logf("[receive packet] <- data: packet=%d, columns=%d, rows=%d", packet, block.NumColumns, block.NumRows)
		case protocol.ServerProfileInfo:
			profileInfo, err := ch.profileInfo()
			if err != nil {
				return err
			}
			ch.logf("[receive packet] <- profiling: rows=%d, bytes=%d, blocks=%d", profileInfo.rows, profileInfo.bytes, profileInfo.blocks)
		case protocol.ServerEndOfStream:
			ch.logf("[receive packet] <- end of stream")
			return nil
		default:
			ch.logf("[receive packet] unexpected packet [%d]", packet)
			return fmt.Errorf("unexpected packet [%d] from server", packet)
		}
	}
}
