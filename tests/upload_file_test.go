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

func TestUploadFile(t *testing.T) {

	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		if protocol == clickhouse.Native {
			return
		}
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)

		ctx := context.Background()
		const ddl = `
			CREATE TABLE test_upload_file (
				  File LowCardinality(FixedString(32))
			    , ID   UInt32
			    , Data Nullable(FixedString(32))

			) Engine MergeTree() ORDER BY (File, ID)
		`

		require.NoError(t, conn.Exec(ctx, ddl))
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_upload_file")
		}()

		tests := []struct {
			Filename    string
			Format      string
			ContentType string
			Encoding    string
			Count       uint64
		}{
			{
				Filename: "./testdata/upload_file.tsv.zstd",
				Format:   "tsV",
				Count:    3,
			},
			{
				Filename:    "./testdata/upload_file.csv.gz",
				Format:      "CSV",
				ContentType: "text/plain; charset=utf-8",
				Encoding:    "gzip",
				Count:       4,
			},
			{
				Filename:    "./testdata/upload_file.json",
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
				opts = append(opts, clickhouse.WithEncoding(test.Encoding))
			} else {
				opts = append(opts, clickhouse.WithFileEncoding(test.Filename))
			}
			f, err := os.Open(test.Filename)
			require.NoError(t, err, test)
			defer f.Close()

			cc := clickhouse.Context(ctx, opts...)

			err = conn.UploadFile(cc, f, fmt.Sprintf("INSERT INTO test_upload_file FORMAT %s", test.Format))
			require.NoError(t, err, test)

			var count uint64
			require.NoError(t, conn.QueryRow(cc, "SELECT Count(*) FROM test_upload_file WHERE File = ?", filepath.Base(test.Filename)).Scan(&count))
			assert.Equal(t, test.Count, count, "Wrong lines count for file", test.Filename)
		}
	})
}
