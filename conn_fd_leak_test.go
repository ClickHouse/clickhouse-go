package clickhouse

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type closeTrackingConn struct {
	net.Conn
	closes *atomic.Int32
}

func (c *closeTrackingConn) Close() error {
	c.closes.Add(1)

	return c.Conn.Close()
}

// TestDialClosesConnection ensures dial closes the socket when the dialer returns
// an error alongside a connection, and when a post-dial setup step fails.
func TestDialClosesConnection(t *testing.T) {
	testCases := map[string]error{
		"setup failure": nil,
		"dial error":    errors.New("dial failed"),
	}

	for name, dialErr := range testCases {
		t.Run(name, func(t *testing.T) {
			client, server := net.Pipe()
			require.NoError(t, server.Close()) // the handshake fails on a closed pipe

			var closes atomic.Int32
			tracked := &closeTrackingConn{Conn: client, closes: &closes}

			_, err := dial(context.Background(), "127.0.0.1:9000", 1, &Options{
				DialContext: func(_ context.Context, _ string) (net.Conn, error) {
					return tracked, dialErr
				},
			})

			require.Error(t, err)
			assert.Equal(t, int32(1), closes.Load(), "dial must close the connection exactly once")
		})
	}
}
