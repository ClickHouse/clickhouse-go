package clickhouse

import "fmt"

func (ch *clickhouse) receivePacket() (*rows, error) {
	var rows rows
	for {
		packet, err := ch.conn.readUInt()
		if err != nil {
			return nil, err
		}
		switch packet {
		case ServerExceptionPacket:
			ch.log("[receive packet] <- exception")
			return nil, ch.exception()
		case ServerProgressPacket:
			progress, err := ch.progress()
			if err != nil {
				return nil, err
			}
			ch.log("[receive packet] <- progress: rows=%d, bytes=%d, total rows=%d",
				progress.bytes,
				progress.rows,
				progress.totalRows,
			)
		case ServerDataPacket:
			datapacket, err := ch.datapacket()
			if err != nil {
				return nil, err
			}
			rows.append(datapacket)
			ch.log("[receive packet] <- datapacket: columns=%d, rows=%d", datapacket.numColumns, datapacket.numRows)
		case ServerExtremesPacket:
			datapacket, err := ch.datapacket()
			if err != nil {
				return nil, err
			}
			ch.log("[receive packet] <- extremes: columns=%d, rows=%d", datapacket.numColumns, datapacket.numRows)
		case ServerTotalsPacket:
			datapacket, err := ch.datapacket()
			if err != nil {
				return nil, err
			}
			ch.log("[receive packet] <- totalpacket: columns=%d, rows=%d", datapacket.numColumns, datapacket.numRows)
		case ServerProfileInfoPacket:
			profileInfo, err := ch.profileInfo()
			if err != nil {
				return nil, err
			}
			ch.log("[receive packet] <- profiling: rows=%d, bytes=%d, blocks=%d", profileInfo.rows, profileInfo.bytes, profileInfo.blocks)
		case ServerEndOfStreamPacket:
			ch.log("[receive packet] <- end of stream")
			return &rows, nil
		default:
			ch.log("[receive packet] unexpected packet [%d]", packet)
			return nil, fmt.Errorf("unexpected packet [%d] from server", packet)
		}
	}
}
