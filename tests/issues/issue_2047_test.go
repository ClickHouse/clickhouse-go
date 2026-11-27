package issues

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/ClickHouse/clickhouse-go/v2/tests/std"
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

			for _, test := range issue2047Cases() {
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

func issue2047Cases() []struct {
	name   string
	values []any
	want   []string
} {
	timestamp := time.Date(2026, 11, 10, 12, 34, 56, 123456789, time.UTC)
	return []struct {
		name   string
		values []any
		want   []string
	}{
		{"array_then_scalar", []any{[]string{"x", "y"}, "z"}, []string{`{"a":{"b":["x","y"]}}`, `{"a":{"b":"z"}}`}},
		{"scalar_then_array", []any{"z", []string{"x", "y"}}, []string{`{"a":{"b":"z"}}`, `{"a":{"b":["x","y"]}}`}},
		{"array_then_empty_scalar", []any{[]string{"x"}, ""}, []string{`{"a":{"b":["x"]}}`, `{"a":{"b":""}}`}},
		{"different_array_elements", []any{[]string{"x"}, []int64{1, 2}}, []string{`{"a":{"b":["x"]}}`, `{"a":{"b":[1,2]}}`}},
		{"alternating_types", []any{[]string{"x"}, "y", []string{"z"}}, []string{`{"a":{"b":["x"]}}`, `{"a":{"b":"y"}}`, `{"a":{"b":["z"]}}`}},
		{"fallback_array_elements", []any{[]string{"x"}, []int64{1}, []any{int64(2)}, []int{3}}, []string{`{"a":{"b":["x"]}}`, `{"a":{"b":[1]}}`, `{"a":{"b":[2]}}`, `{"a":{"b":[3]}}`}},
		{"explicit_array_then_scalar", []any{clickhouse.NewDynamicWithType([]string{"x"}, "Array(String)"), ""}, []string{`{"a":{"b":["x"]}}`, `{"a":{"b":""}}`}},
		{"explicit_time_precision", []any{clickhouse.NewDynamicWithType(timestamp, "DateTime64(9)"), timestamp.Add(time.Nanosecond)}, []string{`{"a":{"b":"2026-11-10 12:34:56.123456789"}}`, `{"a":{"b":"2026-11-10 12:34:56.123456790"}}`}},
	}
}

func Test2047Std(t *testing.T) {
	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprint(protocol), func(t *testing.T) {
			conn, err := std.GetDSNConnection(testSet, protocol, false, url.Values{
				"allow_experimental_json_type": {"1"},
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			if !std.CheckMinServerVersion(conn, 24, 8, 0) {
				t.Skip("JSON requires ClickHouse 24.8 or later")
			}
			for _, test := range issue2047Cases() {
				t.Run(test.name, func(t *testing.T) {
					table := fmt.Sprintf("test_2047_std_%v_%s", protocol, test.name)
					ctx := context.Background()
					_, err := conn.ExecContext(ctx, "CREATE TABLE "+table+" (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id")
					require.NoError(t, err)
					t.Cleanup(func() {
						_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+table)
						require.NoError(t, err)
					})

					insertCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					tx, err := conn.BeginTx(insertCtx, nil)
					require.NoError(t, err)
					defer func() {
						if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
							require.NoError(t, err)
						}
					}()
					batch, err := tx.PrepareContext(insertCtx, "INSERT INTO "+table)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, batch.Close()) })
					for i, value := range test.values {
						_, err := batch.ExecContext(insertCtx, uint32(i), map[string]any{"a": map[string]any{"b": value}})
						require.NoError(t, err)
					}
					require.NoError(t, tx.Commit())

					rows, err := conn.QueryContext(ctx, "SELECT toJSONString(j) FROM "+table+" ORDER BY id")
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
