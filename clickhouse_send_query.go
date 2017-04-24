package clickhouse

func (ch *clickhouse) sendQuery(query string) error {
	ch.logf("[send query] %s", query)
	if err := writeUvarint(ch.conn, ClientQueryPacket); err != nil {
		return err
	}
	if err := writeString(ch.conn, ""); err != nil {
		return err
	}
	if ch.serverRevision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO {
		writeUvarint(ch.conn, 1)
		writeString(ch.conn, "")
		writeString(ch.conn, "") //initial_query_id
		writeString(ch.conn, "[::ffff:127.0.0.1]:0")
		writeUvarint(ch.conn, 1) // iface type TCP
		writeString(ch.conn, hostname)
		writeString(ch.conn, "localhost")
		writeString(ch.conn, ClientName)
		writeUvarint(ch.conn, ClickHouseDBMSVersionMajor)
		writeUvarint(ch.conn, ClickHouseDBMSVersionMinor)
		writeUvarint(ch.conn, ClickHouseRevision)
		if ch.serverRevision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
			writeString(ch.conn, "")
		}
	}
	if err := writeString(ch.conn, ""); err != nil { // settings
		return err
	}
	if err := writeUvarint(ch.conn, StateComplete); err != nil {
		return err
	}
	if err := writeUvarint(ch.conn, 0); err != nil { // compress
		return err
	}
	if err := writeString(ch.conn, query); err != nil {
		return err
	}
	return (&block{}).write(ch.serverRevision, ch.conn)
}
