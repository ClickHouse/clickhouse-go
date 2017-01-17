package clickhouse

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"strings"
)

type stmt struct {
	ch           *clickhouse
	rows         rows
	isInsert     bool
	query        string
	numInput     int
	columnsTypes []string
	datapacket   *datapacket
}

func (stmt *stmt) NumInput() int {
	if stmt.numInput < 0 {
		return 0
	}
	return stmt.numInput
}

func (stmt *stmt) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.isInsert {
		stmt.ch.conn.writeUInt(ClientDataPacket)
		stmt.ch.conn.writeString("") //tmp
		stmt.datapacket.blockInfo.write(stmt.ch.conn)
		stmt.ch.conn.writeUInt(stmt.datapacket.numColumns)
		stmt.ch.conn.writeUInt(2)
	}

	for _, name := range []string{"os_id", "browser_id"} {
		fmt.Println("Write", name)
		stmt.ch.conn.writeString(name)
		stmt.ch.conn.writeString("UInt8")
		fmt.Println(binary.Write(stmt.ch.conn, binary.LittleEndian, uint8(44)))
		fmt.Println(binary.Write(stmt.ch.conn, binary.LittleEndian, uint8(88)))
	}

	fmt.Println("DONE", stmt.ch.ping())
	return &result{}, nil
}

func (stmt *stmt) Query(args []driver.Value) (driver.Rows, error) {
	var query []string
	for index, value := range strings.Split(stmt.query, "?") {
		query = append(query, value)
		if index < len(args) {
			query = append(query, quote(args[index]))
		}
	}
	if err := stmt.ch.sendQuery(strings.Join(query, "")); err != nil {
		return nil, err
	}
	if err := stmt.receivePacket(); err != nil {
		return nil, err
	}
	return &stmt.rows, nil
}

func (stmt *stmt) Close() error {
	stmt.ch.log("[stmt] close")
	stmt.rows = rows{}
	return nil
}

func (stmt *stmt) receivePacket() error {
	for {
		packet, err := stmt.ch.conn.readUInt()
		if err != nil {
			return err
		}
		switch packet {
		case ServerExceptionPacket:
			stmt.ch.log("[stmt] <- exception")
			return stmt.ch.exception()
		case ServerProgressPacket:
			progress, err := stmt.ch.progress()
			if err != nil {
				return err
			}
			stmt.ch.log("[stmt] <- progress: rows=%d, bytes=%d, total rows=%d",
				progress.bytes,
				progress.rows,
				progress.totalRows,
			)
		case ServerDataPacket:
			datapacket, err := stmt.ch.datapacket()
			if err != nil {
				return err
			}
			stmt.rows.append(datapacket)
			stmt.ch.log("[stmt] <- datapacket: columns=%d, rows=%d", datapacket.numColumns, datapacket.numRows)
		case ServerExtremesPacket:
			datapacket, err := stmt.ch.datapacket()
			if err != nil {
				return err
			}
			stmt.rows.append(datapacket)
			stmt.ch.log("[stmt] <- extremes: columns=%d, rows=%d", datapacket.numColumns, datapacket.numRows)
		case ServerTotalsPacket:
			datapacket, err := stmt.ch.datapacket()
			if err != nil {
				return err
			}
			stmt.rows.append(datapacket)
			stmt.ch.log("[stmt] <- totalpacket: columns=%d, rows=%d", datapacket.numColumns, datapacket.numRows)
		case ServerProfileInfoPacket:
			profileInfo, err := stmt.ch.profileInfo()
			if err != nil {
				return err
			}
			stmt.ch.log("[stmt] <- profiling: rows=%d, bytes=%d, blocks=%d", profileInfo.rows, profileInfo.bytes, profileInfo.blocks)
		case ServerEndOfStreamPacket:
			stmt.ch.log("[stmt] <- end of stream")
			return nil
		default:
			stmt.ch.log("[stmt] unexpected packet [%d]", packet)
			return fmt.Errorf("Unexpected packet from server")
		}
	}
	return nil
}
