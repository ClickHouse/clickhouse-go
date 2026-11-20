package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func Test1839(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			for _, test := range []struct {
				name    string
				query   string
				want    string
				variant bool
			}{
				{
					name:    "variant type with quoted timezone",
					query:   `SELECT variantType(CAST(toDateTime('2024-01-15 10:30:00', 'UTC'), 'Variant(String, DateTime(\'UTC\'))'))`,
					want:    "DateTime('UTC')",
					variant: true,
				},
				{
					name:  "second enum label with quote",
					query: `SELECT CAST('c\'d' AS Enum8('a\'b' = 1, 'c\'d' = 2))`,
					want:  "c'd",
				},
				{
					name:  "enum label with multiple quotes",
					query: `SELECT CAST('x\'y\'z' AS Enum8('x\'y\'z' = 1))`,
					want:  "x'y'z",
				},
				{
					name:  "enum label with one quote",
					query: `SELECT CAST('a\'b' AS Enum8('a\'b' = 1))`,
					want:  "a'b",
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					ctx := context.Background()
					if test.variant {
						clickhouse_tests.SkipOnCloud(t, "cannot modify Variant settings on cloud")
						if !clickhouse_tests.CheckMinServerServerVersion(conn, 24, 4, 0) {
							t.Skip("unsupported clickhouse version for Variant type")
						}
						ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
							"allow_experimental_variant_type": true,
							"allow_suspicious_variant_types":  true,
						}))
					}

					var got string
					require.NoError(t, conn.QueryRow(ctx, test.query).Scan(&got))
					require.Equal(t, test.want, got)
				})
			}
		})
	}
}
