package clickhouse

import (
	"bytes"
	"context"
	"database/sql/driver"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func TestQueryProtocolErrorRetiresConnection(t *testing.T) {
	exception := new(chproto.Buffer)
	exception.PutByte(proto.ServerException)
	exception.PutInt32(60)
	exception.PutString("UNKNOWN_TABLE")
	exception.PutString("Table does not exist")
	exception.PutString("")
	exception.PutBool(false)

	for _, phase := range []string{"first block", "stream"} {
		t.Run(phase, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				payload []byte
				closed  bool
			}{
				{"unknown packet with unread response", append([]byte{42}, exception.Buf...), true},
				{"transport EOF", nil, true},
				{"truncated exception", []byte{proto.ServerException, 60}, true},
				{"server exception", exception.Buf, false},
				{"end of stream", []byte{proto.ServerEndOfStream}, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					client, server := net.Pipe()
					require.NoError(t, server.SetReadDeadline(time.Now().Add(time.Second)))
					c := &connect{
						conn: client, reader: chproto.NewReader(bytes.NewReader(tc.payload)),
						logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
						readTimeout: time.Second,
					}
					t.Cleanup(func() {
						require.NoError(t, c.close())
						require.NoError(t, server.Close())
					})
					var err error
					if phase == "first block" {
						_, err = c.firstBlock(context.Background(), &onProcess{})
					} else {
						err = c.process(context.Background(), &onProcess{})
					}
					if tc.name == "end of stream" {
						if phase == "first block" {
							require.ErrorIs(t, err, io.EOF)
						} else {
							require.NoError(t, err)
						}
					} else {
						require.Error(t, err)
						require.NotErrorIs(t, err, driver.ErrBadConn, "a query may already have executed")
					}
					require.Equal(t, tc.closed, c.isClosed(), "retire before exposing the error to the caller")
					if tc.closed {
						_, readErr := server.Read(make([]byte, 1))
						require.ErrorIs(t, readErr, io.EOF, "close the socket, not just the pool state")
					}
				})
			}
		})
	}
}
