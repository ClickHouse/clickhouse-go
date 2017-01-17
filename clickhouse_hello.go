package clickhouse

import (
	"fmt"
	"time"
)

func (ch *clickhouse) hello(database, username, password string) error {
	ch.log("[hello] -> %s %d.%d.%d",
		ClientName,
		ClickHouseDBMSVersionMajor,
		ClickHouseDBMSVersionMinor,
		ClickHouseRevision,
	)
	{
		ch.conn.writeUInt(ClientHelloPacket)
		ch.conn.writeString(ClientName)
		ch.conn.writeUInt(ClickHouseDBMSVersionMajor)
		ch.conn.writeUInt(ClickHouseDBMSVersionMinor)
		ch.conn.writeUInt(ClickHouseRevision)
		ch.conn.writeString(database)
		ch.conn.writeString(username)
		ch.conn.writeString(password)
	}
	{
		packet, err := ch.conn.readUInt()
		if err != nil {
			return err
		}
		switch packet {
		case ServerExceptionPacket:
			return ch.exception()
		case ServerHelloPacket:
			var err error
			if ch.serverName, err = ch.conn.readString(); err != nil {
				return err
			}
			if ch.serverVersionMinor, err = ch.conn.readUInt(); err != nil {
				return err
			}
			if ch.serverVersionMajor, err = ch.conn.readUInt(); err != nil {
				return err
			}
			if ch.serverRevision, err = ch.conn.readUInt(); err != nil {
				return err
			}
			if ch.serverRevision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
				timezone, err := ch.conn.readString()
				if err != nil {
					return err
				}
				if ch.serverTimezone, err = time.LoadLocation(timezone); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("Unexpected packet from server")
		}
	}
	ch.log("[hello] <- %s %d.%d.%d (%s)",
		ch.serverName,
		ch.serverVersionMajor,
		ch.serverVersionMinor,
		ch.serverRevision,
		ch.serverTimezone,
	)
	return nil
}
