package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/stretchr/testify/assert"
)

func TestConn(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug: true,
	})
	if assert.NoError(t, err) {
		if err := conn.Ping(context.Background()); assert.NoError(t, err) {
			if assert.NoError(t, conn.Close()) {
				t.Log(conn.Stats())
				t.Log(conn.ServerVersion())
			}
		}
	}
}

func TestPingDeadline(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug: true,
	})
	if assert.NoError(t, err) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()
		if err := conn.Ping(ctx); assert.Error(t, err) {
			assert.Contains(t, err.Error(), "i/o timeout")
		}
	}
}

func TestExec(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug: true,
	})

	if assert.NoError(t, err) {
		ctx := context.Background()
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_exec")
		conn.Exec(ctx, `
		CREATE TABLE test_exec (
			Column1 UInt8
		) Engine = Memory
		`)

		conn.Exec(ctx, `INSERT INTO test_exec (Column1)
			SELECT 1 FROM system.numbers LIMIT 200
		`)
		assert.NoError(t, conn.Close())
	}
}
func TestContext(t *testing.T) {
	progress := make(chan clickhouse.Progress)
	clickhouse.Context(context.Background(),
		clickhouse.WithProgress(progress),
		clickhouse.WithSettings(clickhouse.Settings{
			"max_execution_time": 256,
		}),
	)
}

func TestQuery(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug: true,
	})
	if assert.NoError(t, err) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
		defer cancel()
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"max_block_size": 3,
		}))
		rows, err := conn.Query(ctx, `
			SELECT
				number AS int
				, number::Nullable(UInt64) AS nullable
			FROM system.numbers
			LIMIT 20`)
		if assert.NoError(t, err) {
			t.Log("columns: ", rows.Columns())
			for rows.Next() {
				var (
					rowInt uint64
					rowNil *uint64
				)
				if err := rows.Scan(&rowInt, &rowNil); assert.NoError(t, err) {
					t.Log("SCANN", rowInt, rowNil)
				}
			}
		}
	}
}

func TestQueryBindNumeric(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug: true,
	})
	if assert.NoError(t, err) {
		rows, err := conn.Query(context.Background(), `
		SELECT
			  $1::Int8
			, $2::Int64
			, $1::UInt8
			, $2::UInt64
		`, 10, 1000)
		if assert.NoError(t, err) {
			for rows.Next() {
				var (
					int8Column   int8
					int64Column  int64
					uint8Column  uint8
					uint64Column uint64
				)
				err := rows.Scan(
					&int8Column,
					&int64Column,
					&uint8Column,
					&uint64Column,
				)
				if assert.NoError(t, err) {
					assert.Equal(t, int8(10), int8Column)
					assert.Equal(t, int64(1000), int64Column)
					assert.Equal(t, uint8(10), uint8Column)
					assert.Equal(t, uint64(1000), uint64Column)
				}
			}
		}
	}
}
