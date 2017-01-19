package clickhouse

func (ch *clickhouse) ping() error {
	ch.log("-> ping")
	if err := writeUvarint(ch.conn, ClientPingPacket); err != nil {
		return err
	}
	if err := ch.gotPacket(ServerPongPacket); err != nil {
		return err
	}
	ch.log("<- pong")
	return nil
}
