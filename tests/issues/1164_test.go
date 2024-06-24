package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func TestIssue1164(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = "CREATE TABLE test_1164 (Col1 String) Engine MergeTree() ORDER BY tuple()"
	err = conn.Exec(ctx, ddl)
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_1164")
	}()

	column.WithAllocBufferColStrProvider(4096)

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_1164")
	require.NoError(t, err)

	for i := 0; i < 10000; i++ {
		appendErr := batch.Append(fmt.Sprintf("some_text_%d", i))
		require.NoError(t, appendErr)
	}

	err = batch.Send()
	require.NoError(t, err)
}

func BenchmarkIssue1164(b *testing.B) {
	// result:
	//cpu: Intel(R) Xeon(R) CPU E5-26xx v4
	//BenchmarkIssue1164
	//BenchmarkIssue1164/default-10000
	//BenchmarkIssue1164/default-10000-8         	     100	  11533744 ns/op	 1992731 B/op	   40129 allocs/op
	//BenchmarkIssue1164/preAlloc-10000
	//BenchmarkIssue1164/preAlloc-10000-8        	     104	  11136623 ns/op	 1991154 B/op	   40110 allocs/op
	//BenchmarkIssue1164/default-50000
	//BenchmarkIssue1164/default-50000-8         	      22	  49932579 ns/op	11592053 B/op	  200150 allocs/op
	//BenchmarkIssue1164/preAlloc-50000
	//BenchmarkIssue1164/preAlloc-50000-8        	      24	  49687163 ns/op	11573934 B/op	  200148 allocs/op
	b.Run("default-10000", func(b *testing.B) {
		var (
			conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
				"max_execution_time": 60,
			}, nil, &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			})
		)
		ctx := context.Background()
		require.NoError(b, err)
		const ddl = "CREATE TABLE test_1164 (Col1 String) Engine MergeTree() ORDER BY tuple()"
		err = conn.Exec(ctx, ddl)
		require.NoError(b, err)
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_1164")
		}()

		b.ReportAllocs()
		for k := 0; k < b.N; k++ {
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_1164")
			require.NoError(b, err)

			for i := 0; i < 10000; i++ {
				appendErr := batch.Append(fmt.Sprintf("some_text_%d", i))
				require.NoError(b, appendErr)
			}

			err = batch.Send()
			require.NoError(b, err)
		}

	})
	b.Run("preAlloc-10000", func(b *testing.B) {
		var (
			conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
				"max_execution_time": 60,
			}, nil, &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			})
		)
		ctx := context.Background()
		require.NoError(b, err)
		const ddl = "CREATE TABLE test_1164 (Col1 String) Engine MergeTree() ORDER BY tuple()"
		err = conn.Exec(ctx, ddl)
		require.NoError(b, err)
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_1164")
		}()

		column.WithAllocBufferColStrProvider(4096)

		b.ReportAllocs()
		for k := 0; k < b.N; k++ {
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_1164")
			require.NoError(b, err)

			for i := 0; i < 10000; i++ {
				appendErr := batch.Append(fmt.Sprintf("some_text_%d", i))
				require.NoError(b, appendErr)
			}

			err = batch.Send()
			require.NoError(b, err)
		}

	})
	b.Run("default-50000", func(b *testing.B) {
		var (
			conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
				"max_execution_time": 60,
			}, nil, &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			})
		)
		ctx := context.Background()
		require.NoError(b, err)
		const ddl = "CREATE TABLE test_1164 (Col1 String) Engine MergeTree() ORDER BY tuple()"
		err = conn.Exec(ctx, ddl)
		require.NoError(b, err)
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_1164")
		}()

		b.ReportAllocs()
		for k := 0; k < b.N; k++ {
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_1164")
			require.NoError(b, err)

			for i := 0; i < 50000; i++ {
				appendErr := batch.Append(fmt.Sprintf("some_text_%d", i))
				require.NoError(b, appendErr)
			}

			err = batch.Send()
			require.NoError(b, err)
		}

	})
	b.Run("preAlloc-50000", func(b *testing.B) {
		var (
			conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
				"max_execution_time": 60,
			}, nil, &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			})
		)
		ctx := context.Background()
		require.NoError(b, err)
		const ddl = "CREATE TABLE test_1164 (Col1 String) Engine MergeTree() ORDER BY tuple()"
		err = conn.Exec(ctx, ddl)
		require.NoError(b, err)
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_1164")
		}()

		column.WithAllocBufferColStrProvider(4096)

		b.ReportAllocs()
		for k := 0; k < b.N; k++ {
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_1164")
			require.NoError(b, err)

			for i := 0; i < 50000; i++ {
				appendErr := batch.Append(fmt.Sprintf("some_text_%d", i))
				require.NoError(b, appendErr)
			}

			err = batch.Send()
			require.NoError(b, err)
		}

	})

}
