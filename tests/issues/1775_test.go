package issues

import (
	"context"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func TestIssue1775_JSONMapScanOmitsAbsentKeys(t *testing.T) {
	ctx := context.Background()

	conn, err := clickhouse_tests.GetConnectionTCP("issues", clickhouse.Settings{
		"allow_experimental_variant_type": true,
		"allow_experimental_dynamic_type": true,
		"allow_experimental_json_type":    true,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err, "open clickhouse")

	if !clickhouse_tests.CheckMinServerServerVersion(conn, 24, 8, 0) {
		t.Skip("unsupported clickhouse version for JSON type")
	}

	const tableName = "test_1775_json_map_scan"
	require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName))
	require.NoError(t, conn.Exec(ctx, `
		CREATE TABLE `+tableName+` (
			id UInt8,
			data JSON
		) Engine = MergeTree() ORDER BY id
	`))
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
	}()

	testCases := []struct {
		name              string
		insertTuple       string
		expected          map[string]any
		notExpected       []string
		expectedNested    map[string]map[string]any
		notExpectedNested map[string][]string
	}{
		{
			name:        "simple non-nested",
			insertTuple: "(1, '{\"a\":\"foo\"}'::JSON)",
			expected:    map[string]any{"a": "foo"},
			notExpected: []string{"b", "x"},
		},
		{
			name:        "simple non-nested2",
			insertTuple: "(2, '{\"b\":\"bar\"}'::JSON)",
			expected:    map[string]any{"b": "bar"},
			notExpected: []string{"a", "x"},
		},
		{
			name:        "simple non-nested with explicit null",
			insertTuple: "(3, '{\"b\":\"bar\", \"d\": null}'::JSON)", // explicit null
			expected:    map[string]any{"b": "bar"},                  // but don't expct key `d`. because clickhouse server doesn't even store the keys with null values
			notExpected: []string{"a", "x"},
		},
		{
			name:        "simple nested",
			insertTuple: "(4, '{\"x\":{\"a\":1}}'::JSON)",
			notExpected: []string{"a", "b"},
			expectedNested: map[string]map[string]any{
				"x": {"a": 1},
			},
			notExpectedNested: map[string][]string{
				"x": {"b"},
			},
		},
		{
			name:        "simple nested2",
			insertTuple: "(5, '{\"x\":{\"b\":2}}'::JSON)",
			notExpected: []string{"a", "b"},
			expectedNested: map[string]map[string]any{
				"x": {"b": 2},
			},
			notExpectedNested: map[string][]string{
				"x": {"a"},
			},
		},
		{
			name:        "simple nested with explicit null",
			insertTuple: "(6, '{\"x\":{\"a\":1, \"d\":null}}'::JSON)", // explicit null
			notExpected: []string{"a", "b"},
			expectedNested: map[string]map[string]any{
				"x": {"a": 1},
			},
			notExpectedNested: map[string][]string{
				"x": {"b", "d"}, // don't expct key `d`. because clickhouse server doesn't even store the keys with null values
			},
		},
	}

	insertTuples := make([]string, len(testCases))
	for i := range testCases {
		insertTuples[i] = testCases[i].insertTuple
	}

	require.NoError(t, conn.Exec(ctx, "INSERT INTO "+tableName+" VALUES\n\t"+strings.Join(insertTuples, ",\n\t")))

	rows, err := conn.Query(ctx, "SELECT id, data FROM "+tableName+" ORDER BY id")
	require.NoError(t, err)

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, rows.Next())

			var (
				id   uint8
				data map[string]any
			)

			require.NoError(t, rows.Scan(&id, &data))
			require.Equal(t, uint8(i+1), id)

			for key, expected := range testCases[i].expected {
				require.EqualValues(t, expected, data[key])
				require.Contains(t, data, key)
			}

			for _, key := range testCases[i].notExpected {
				require.NotContains(t, data, key)
			}

			for parent, expectedChildren := range testCases[i].expectedNested {
				childMap, ok := data[parent].(map[string]any)
				require.True(t, ok)
				for key, expected := range expectedChildren {
					require.EqualValues(t, expected, childMap[key])
					require.Contains(t, childMap, key)
				}
			}

			for parent, absentChildren := range testCases[i].notExpectedNested {
				childMap, ok := data[parent].(map[string]any)
				require.True(t, ok)
				for _, key := range absentChildren {
					require.NotContains(t, childMap, key)
				}
			}
		})
	}

	require.False(t, rows.Next())
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
}
