package clickhouse

func (ch *clickhouse) sendQuery(query string) error {
	ch.log("[send query] %s", query)
	if err := ch.conn.writeUInt(ClientQueryPacket); err != nil {
		return err
	}
	if err := ch.conn.writeString(""); err != nil { // queryID
		return err
	}
	if ch.serverRevision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO {
		ch.conn.writeUInt(1)
		ch.conn.writeString("")
		ch.conn.writeString("") //initial_query_id
		ch.conn.writeString("[::ffff:127.0.0.1]:0")
		ch.conn.writeUInt(1) // iface type TCP
		ch.conn.writeString(hostname)
		ch.conn.writeString("localhost")
		ch.conn.writeString(ClientName)
		ch.conn.writeUInt(ClickHouseDBMSVersionMajor)
		ch.conn.writeUInt(ClickHouseDBMSVersionMinor)
		ch.conn.writeUInt(ClickHouseRevision)
		if ch.serverRevision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
			ch.conn.writeString("")
		}
	}
	if err := ch.conn.writeString(""); err != nil { // settings
		return err
	}
	if err := ch.conn.writeUInt(StateComplete); err != nil {
		return err
	}
	if err := ch.conn.writeUInt(0); err != nil { // compress
		return err
	}
	if err := ch.conn.writeString(query); err != nil {
		return err
	}
	{ // datablock
		if err := ch.conn.writeUInt(ClientDataPacket); err != nil {
			return err
		}
		if ch.serverRevision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
			if err := ch.conn.writeString(""); err != nil {
				return err
			}
		}
		for _, z := range []uint{0, 0, 0} { // empty block
			if err := ch.conn.writeUInt(z); err != nil {
				return err
			}
		}
	}
	return nil
}
