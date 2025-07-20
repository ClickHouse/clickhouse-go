package clickhouse

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	chtesting "github.com/ClickHouse/clickhouse-go/v2/lib/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConn_Query(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(
		os.Stderr,
		&slog.HandlerOptions{
			Level: slog.LevelDebug,
		},
	)))

	handlers := chtesting.DefaultHandlers()
	handlers.OnQuery = func(q *proto.Query, blocks []*proto.Block, c chan<- *proto.Block) error {
		col := &column.UInt8{}
		require.NoError(t, col.AppendRow(uint8(1)))

		c <- &proto.Block{
			Columns: []column.Interface{
				col,
			},
		}

		return nil
	}

	server, err := chtesting.NewTestServer(":0", handlers)
	require.NoError(t, err)

	server.Start()
	t.Cleanup(func() { server.Stop() })

	conn, err := Open(&Options{
		Addr:         []string{server.Address()},
		MaxOpenConns: 2,
	})
	require.NoError(t, err)

	ctx := context.TODO()
	require.NoError(t, conn.Ping(ctx))

	rows, err := conn.Query(ctx, "SELECT 1")
	require.NoError(t, err)

	var num uint8
	for rows.Next() {
		err := rows.Scan(&num)
		require.NoError(t, err)
	}

	assert.Equal(t, uint8(1), num)
	require.NoError(t, rows.Err())
}

func TestConn_Query_ReadTimeout(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(
		os.Stderr,
		&slog.HandlerOptions{
			Level: slog.LevelDebug,
		},
	)))

	handlers := chtesting.DefaultHandlers()
	handlers.OnQuery = func(q *proto.Query, blocks []*proto.Block, c chan<- *proto.Block) error {
		col := &column.UInt8{}
		require.NoError(t, col.AppendRow(uint8(1)))

		// sends first block
		c <- &proto.Block{
			Columns: []column.Interface{
				col,
			},
		}

		// then blocks indefinitely
		select {}
	}

	server, err := chtesting.NewTestServer(":0", handlers)
	require.NoError(t, err)

	server.Start()
	t.Cleanup(func() { server.Stop() })

	conn, err := Open(&Options{
		Addr:         []string{server.Address()},
		MaxOpenConns: 2,
		ReadTimeout:  2 * time.Second,
	})
	require.NoError(t, err)

	ctx := context.TODO()
	require.NoError(t, conn.Ping(ctx))

	rows, err := conn.Query(ctx, "SELECT 1")
	require.NoError(t, err)

	t.Run("first row is returned", func(t *testing.T) {
		assert.True(t, rows.Next())
		var num uint8
		err = rows.Scan(&num)
		require.NoError(t, err)
		assert.Equal(t, uint8(1), num)
	})

	t.Run("second row timeout", func(t *testing.T) {
		assert.False(t, rows.Next())
		err := rows.Err()
		assert.True(t, isDeadlineExceededError(err), "error is not a timeout error: %#v", err)
	})
}

type timeout interface {
	Timeout() bool
}

func isDeadlineExceededError(err error) bool {
	nerr, ok := errors.Unwrap(err).(timeout)
	if !ok {
		return false
	}

	return nerr.Timeout()
}
