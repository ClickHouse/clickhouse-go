package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func benchmark1685(ctx context.Context, conn clickhouse.Conn) error {
	for i := 0; i < 10_000; i++ {
		err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO test_xxxx VALUES (
			%d, '%s', [1, 2, 3, 4, 5, 6, 7, 8, 9], now()
		)`, i, "Golang SQL database driver"), false)
		if err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkIssue1685(b *testing.B) {
	conn, err := tests.GetConnectionTCP("issues", nil, nil, nil)
	ctx := context.Background()
	require.NoError(b, err)

	const ddl = `CREATE TABLE test_xxxx (Col1 UInt64, Col2 String, Col3 Array(UInt8), Col4 DateTime) Engine ReplacingMergeTree() ORDER BY Col1`
	err = conn.Exec(ctx, ddl)
	require.NoError(b, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_xxxx")
	}()

	for k := 0; k < b.N; k++ {
		require.NoError(b, benchmark1685(ctx, conn))
	}
}
