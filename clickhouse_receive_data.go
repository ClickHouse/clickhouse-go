package clickhouse

import "fmt"

func (ch *clickhouse) receiveData() (*rows, error) {
	var rows rows
	for {
		packet, err := readUvarint(ch.conn)
		if err != nil {
			return nil, err
		}
		switch packet {
		case ServerExceptionPacket:
			ch.logf("[receive packet] <- exception")
			return nil, ch.exception()
		case ServerProgressPacket:
			progress, err := ch.progress()
			if err != nil {
				return nil, err
			}
			ch.logf("[receive packet] <- progress: rows=%d, bytes=%d, total rows=%d",
				progress.bytes,
				progress.rows,
				progress.totalRows,
			)
		case ServerDataPacket:
			var block block
			if err := block.read(ch.serverRevision, ch.conn); err != nil {
				ch.logf("[receive packet] err: %v", err)
				return nil, err
			}
			if block.numRows > 0 {
				rows.append(&block)
			}
			ch.logf("[receive packet] <- data: columns=%d, rows=%d", block.numColumns, block.numRows)
		case ServerExtremesPacket:
			var block block
			if err := block.read(ch.serverRevision, ch.conn); err != nil {
				return nil, err
			}
			ch.logf("[receive packet] <- extremes: columns=%d, rows=%d", block.numColumns, block.numRows)
		case ServerTotalsPacket:
			var block block
			if err := block.read(ch.serverRevision, ch.conn); err != nil {
				return nil, err
			}
			if block.numRows > 0 {
				rows.append(&block)
			}
			ch.logf("[receive packet] <- totals: columns=%d, rows=%d", block.numColumns, block.numRows)
		case ServerProfileInfoPacket:
			profileInfo, err := ch.profileInfo()
			if err != nil {
				return nil, err
			}
			ch.logf("[receive packet] <- profiling: rows=%d, bytes=%d, blocks=%d", profileInfo.rows, profileInfo.bytes, profileInfo.blocks)
		case ServerEndOfStreamPacket:
			ch.logf("[receive packet] <- end of stream")
			return &rows, nil
		default:
			ch.logf("[receive packet] unexpected packet [%d]", packet)
			return nil, fmt.Errorf("unexpected packet [%d] from server", packet)
		}
	}
}
