package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func TestStructIterProtocols(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		const table = "test_struct_iter"
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+table))
		t.Cleanup(func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+table))
		})
		require.NoError(t, conn.Exec(ctx, `
			CREATE TABLE test_struct_iter (
				Col1 UInt8,
				Col2 String
			) ENGINE = Memory
		`))

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_struct_iter")
		require.NoError(t, err)
		require.NoError(t, batch.Append(uint8(1), "one"))
		require.NoError(t, batch.Append(uint8(2), "two"))
		require.NoError(t, batch.Send())

		rows, err := conn.Query(ctx, "SELECT Col1, Col2 FROM test_struct_iter ORDER BY Col1")
		require.NoError(t, err)

		type result struct {
			Col1 uint8
			Col2 string
		}
		var got []result
		for value, err := range chdriver.StructIter[result](rows) {
			require.NoError(t, err)
			got = append(got, value)
		}

		require.Equal(t, []result{
			{Col1: 1, Col2: "one"},
			{Col1: 2, Col2: "two"},
		}, got)
	})
}
