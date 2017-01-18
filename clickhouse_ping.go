package clickhouse

import "fmt"

func (ch *clickhouse) ping() error {
	ch.log("-> ping")
	if err := ch.conn.writeUInt(ClientPingPacket); err != nil {
		return err
	}
	packet, err := ch.conn.readUInt()
	if err != nil {
		return err
	}
	for packet == ServerProgressPacket {
		if _, err = ch.progress(); err != nil {
			return err
		}
		if packet, err = ch.conn.readUInt(); err != nil {
			return err
		}
	}
	if packet != ServerPongPacket {
		return fmt.Errorf("unexpected packet [%d] from server", packet)
	}
	ch.log("<- pong")
	return nil
}
