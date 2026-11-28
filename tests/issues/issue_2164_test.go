package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test2164 verifies that QueryRow gives the connection back to the pool when Scan or
// ScanStruct fails on a result of several blocks.
// See https://github.com/ClickHouse/clickhouse-go/issues/2164.
func Test2164(t *testing.T) {
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)

	// One row per block, so the result does not fit into the block buffer. The first
	// value is NULL, which can't be scanned into a string.
	const query = "SELECT if(number = 0, NULL, toString(number)) AS value FROM numbers(10)"
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"max_block_size": 1,
	}))

	scans := map[string]struct {
		scan    func(row driver.Row) error
		wantErr string
	}{
		"Scan": {
			scan: func(row driver.Row) error {
				var value string
				return row.Scan(&value)
			},
			wantErr: "converting NULL to *string is unsupported",
		},
		"ScanStruct": {
			scan: func(row driver.Row) error {
				var dest struct {
					Other string `ch:"other"`
				}
				return row.ScanStruct(&dest)
			},
			wantErr: `missing destination name "value"`,
		},
	}

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		for name, tc := range scans {
			t.Run(fmt.Sprintf("%v/%s", protocol, name), func(t *testing.T) {
				options := clickhouse_tests.ClientOptionsFromEnv(env, nil, protocol == clickhouse.HTTP)
				options.MaxOpenConns = 1
				conn, err := clickhouse_tests.GetConnectionWithOptions(&options)
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })

				require.ErrorContains(t, tc.scan(conn.QueryRow(ctx, query)), tc.wantErr)
				var one uint8
				require.NoError(t, conn.QueryRow(context.Background(), "SELECT 1").Scan(&one))
			})
		}
	}
}
