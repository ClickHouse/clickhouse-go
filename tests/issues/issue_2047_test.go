package issues

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/tests"
)

func Test2047(t *testing.T) {
	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprint(protocol), func(t *testing.T) {
			conn, err := tests.GetConnection(testSet, t, protocol, clickhouse.Settings{
				"allow_experimental_json_type": true,
			}, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			if !tests.CheckMinServerServerVersion(conn, 24, 8, 0) {
				t.Skip("JSON requires ClickHouse 24.8 or later")
			}

			for _, test := range []struct {
				name   string
				values []any
				want   []string
			}{
				{"array_then_scalar", []any{[]string{"x", "y"}, "z"}, []string{`{"a":{"b":["x","y"]}}`, `{"a":{"b":"z"}}`}},
				{"scalar_then_array", []any{"z", []string{"x", "y"}}, []string{`{"a":{"b":"z"}}`, `{"a":{"b":["x","y"]}}`}},
				{"array_then_empty_scalar", []any{[]string{"x"}, ""}, []string{`{"a":{"b":["x"]}}`, `{"a":{"b":""}}`}},
				{"different_array_elements", []any{[]string{"x"}, []int64{1, 2}}, []string{`{"a":{"b":["x"]}}`, `{"a":{"b":[1,2]}}`}},
				{"alternating_types", []any{[]string{"x"}, "y", []string{"z"}}, []string{`{"a":{"b":["x"]}}`, `{"a":{"b":"y"}}`, `{"a":{"b":["z"]}}`}},
			} {
				t.Run(test.name, func(t *testing.T) {
					table := fmt.Sprintf("test_2047_%v_%s", protocol, test.name)
					ctx := context.Background()
					require.NoError(t, conn.Exec(ctx, "CREATE TABLE "+table+" (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id"))
					t.Cleanup(func() { require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+table)) })

					insertCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					batch, err := conn.PrepareBatch(insertCtx, "INSERT INTO "+table)
					require.NoError(t, err)
					for i, value := range test.values {
						require.NoError(t, batch.Append(uint32(i), map[string]any{"a": map[string]any{"b": value}}))
					}
					require.NoError(t, batch.Send())

					rows, err := conn.Query(ctx, "SELECT toJSONString(j) FROM "+table+" ORDER BY id")
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, rows.Close()) })
					var got []string
					for rows.Next() {
						var value string
						require.NoError(t, rows.Scan(&value))
						got = append(got, value)
					}
					require.NoError(t, rows.Err())
					require.Len(t, got, len(test.want))
					for i, want := range test.want {
						require.JSONEq(t, want, got[i])
					}
				})
			}
		})
	}
}
