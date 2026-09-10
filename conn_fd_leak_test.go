package clickhouse

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type closeTrackingConn struct {
	net.Conn
	closed *atomic.Bool
}

func (c *closeTrackingConn) Close() error {
	c.closed.Store(true)

	return c.Conn.Close()
}

// TestDialClosesConnectionOnSetupFailure ensures dial does not leak the underlying
// socket (and its file descriptor) when a post-dial setup step fails. The server
// side of the pipe is closed up front so the handshake fails.
func TestDialClosesConnectionOnSetupFailure(t *testing.T) {
	client, server := net.Pipe()
	require.NoError(t, server.Close())

	var closed atomic.Bool
	tracked := &closeTrackingConn{Conn: client, closed: &closed}

	_, err := dial(context.Background(), "127.0.0.1:9000", 1, &Options{
		DialContext: func(_ context.Context, _ string) (net.Conn, error) {
			return tracked, nil
		},
	})

	require.Error(t, err)
	assert.True(t, closed.Load(), "dial must close the connection when a post-dial setup step fails")
}
