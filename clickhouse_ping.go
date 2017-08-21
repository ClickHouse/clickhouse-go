package clickhouse

import (
	"context"

	"github.com/kshvakov/clickhouse/lib/protocol"
)

func (ch *clickhouse) Ping(ctx context.Context) error {
	return ch.ping()
}

func (ch *clickhouse) ping() error {
	ch.logf("-> ping")
	if err := ch.encoder.Uvarint(protocol.ClientPing); err != nil {
		return err
	}
	if err := ch.buffer.Flush(); err != nil {
		return err
	}
	return ch.process()
}
