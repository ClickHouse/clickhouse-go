package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestFileInsert(t *testing.T) {

	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		if protocol == clickhouse.Native {
			return
		}
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)

		d, _ := os.Getwd()
		fmt.Println("PWD: " + d)

		ctx := context.Background()
		const ddl = `
			CREATE TABLE test_file_insert (
				  File LowCardinality(FixedString(32))
			    , ID   UInt32
			    , Data Nullable(FixedString(32))

			) Engine MergeTree() ORDER BY (File, ID)
		`

		require.NoError(t, conn.Exec(ctx, ddl))
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_file_insert")
		}()

		tests := []struct {
			Filename    string
			Format      string
			ContentType string
			Encoding    string
			Count       uint64
		}{
			{
				Filename: "./testdata/file_insert.tsv.zstd",
				Format:   "tsV",
				Count:    3,
			},
			{
				Filename:    "./testdata/file_insert.csv.gz",
				Format:      "CSV",
				ContentType: "text/plain; charset=utf-8",
				Encoding:    "gzip",
				Count:       4,
			},
			{
				Filename:    "./testdata/file_insert.json",
				Format:      "JSONEachRow",
				ContentType: "application/json; charset=utf-8",
				Count:       5,
			},
		}
		for _, test := range tests {
			opts := make([]clickhouse.QueryOption, 0, 4)
			if test.ContentType != "" {
				opts = append(opts, clickhouse.WithFileContentType(test.ContentType))
			}
			if test.Encoding != "" {
				opts = append(opts, clickhouse.WithFileEncoding(test.Encoding))
			}
			ctx = clickhouse.Context(ctx, opts...)
			err := conn.InsertFile(ctx, test.Filename, fmt.Sprintf("INSERT INTO test_file_insert FORMAT %s", test.Format))
			require.NoError(t, err, test)

			var count uint64
			require.NoError(t, conn.QueryRow(ctx, "SELECT Count(*) FROM test_file_insert WHERE File = ?", filepath.Base(test.Filename)).Scan(&count))
			assert.Equal(t, test.Count, count, "Wrong lines count for file", test.Filename)
		}
	})
}
