package clickhouse

import (
	"io"
	"log/slog"
	"net"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func TestSendAddendumQuotaKey(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() {
		require.NoError(t, client.Close())
		require.NoError(t, server.Close())
	})
	c := &connect{
		conn:     client,
		buffer:   new(chproto.Buffer),
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		opt:      &Options{QuotaKey: "tenant_a"},
		revision: proto.DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- c.sendAddendum() }()

	quotaKey, err := chproto.NewReader(server).Str()
	require.NoError(t, err)
	require.NoError(t, <-errCh)
	require.Equal(t, "tenant_a", quotaKey)
}
