package issues

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1841_JSONSerializationVersion locks the behaviors PR #1850
// shipped while fixing issue #1841. Each subtest pins a previously-broken
// case from the PR description so a future refactor cannot silently
// regress it.
//
// The column-layer mode-latch logic is protocol-agnostic — both Native
// (TCP) and HTTP go through the same Append/AppendRow code paths and the
// same WriteStatePrefix encoder for native blocks — so we run every
// subtest on both protocols.
func TestIssue1841_JSONSerializationVersion(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		ctx := context.Background()

		conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse.Settings{
			"allow_experimental_variant_type": true,
			"allow_experimental_dynamic_type": true,
			"allow_experimental_json_type":    true,
			// `toString(JSON)` quotes 64-bit integers by default on ClickHouse < 25.4
			// and emits them unquoted on newer servers. Pin the setting so the
			// round-trip assertions don't depend on server version.
			"output_format_json_quote_64bit_integers": false,
		}, nil, nil)
		require.NoError(t, err, "open clickhouse")

		const jsonText = `{"id":1234,"name":"Book","tags":["a","b"]}`

		// 1. *string holding JSON text is preserved end-to-end.
		// PR #1850 §1: previously the pointer was misclassified as object mode
		// and stored as "{}" — silent data loss.
		t.Run("string pointer round-trips JSON content", func(t *testing.T) {
			if !clickhouse_tests.CheckMinServerServerVersion(conn, 24, 8, 0) {
				t.Skip("JSON unsupported on this server version")
			}
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_1841_string_ptr"))
			require.NoError(t, conn.Exec(ctx, `
				CREATE TABLE test_1841_string_ptr (product JSON)
				ENGINE = MergeTree ORDER BY tuple()
			`))
			t.Cleanup(func() { _ = conn.Exec(ctx, "DROP TABLE IF EXISTS test_1841_string_ptr") })

			ptr := jsonText
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_1841_string_ptr (product)")
			require.NoError(t, err)
			require.NoError(t, batch.Append(&ptr))
			require.NoError(t, batch.Send())

			var stored string
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT toString(product) FROM test_1841_string_ptr").Scan(&stored))
			require.Equal(t, jsonText, stored,
				"*string content must round-trip — PR #1850 §1 fix; previously stored as {}")
		})

		// 2. Append(nil) does not latch the serialization mode, so either
		// ordering — nil first or string first — succeeds. PR #1850 §2.
		for _, tc := range []struct {
			name        string
			nilFirst    bool
			expectFirst string
			expectLast  string
		}{
			{"nil then string", true, "", jsonText},
			{"string then nil", false, jsonText, ""},
		} {
			t.Run("ordering — "+tc.name, func(t *testing.T) {
				if !clickhouse_tests.CheckMinServerServerVersion(conn, 25, 2, 0) {
					t.Skip("Nullable(JSON) requires server 25.2+")
				}
				tableName := "test_1841_order_" + strings.ReplaceAll(tc.name, " ", "_")
				require.NoError(t, conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)))
				require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
					CREATE TABLE %s (id UInt32, product Nullable(JSON))
					ENGINE = MergeTree ORDER BY id
				`, tableName)))
				t.Cleanup(func() { _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })

				batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, product)", tableName))
				require.NoError(t, err)

				if tc.nilFirst {
					require.NoError(t, batch.Append(uint32(1), nil))
					require.NoError(t, batch.Append(uint32(2), jsonText))
				} else {
					require.NoError(t, batch.Append(uint32(1), jsonText))
					require.NoError(t, batch.Append(uint32(2), nil))
				}
				require.NoError(t, batch.Send(),
					"Append(nil) must not pre-latch the serialization mode — PR #1850 §2")

				rows, err := conn.Query(ctx, fmt.Sprintf("SELECT id, toString(product) FROM %s ORDER BY id", tableName))
				require.NoError(t, err)
				defer rows.Close()

				gotByID := map[uint32]string{}
				for rows.Next() {
					var id uint32
					var s *string
					require.NoError(t, rows.Scan(&id, &s))
					if s == nil {
						gotByID[id] = ""
					} else {
						gotByID[id] = *s
					}
				}
				require.NoError(t, rows.Err())
				require.Equal(t, tc.expectFirst, gotByID[1], "row id=1")
				require.Equal(t, tc.expectLast, gotByID[2], "row id=2")
			})
		}

		// 3. Mixing object and string modes in one block returns an error.
		// PR #1850 §3: previously the second value was silently lost.
		t.Run("mixed object then string errors", func(t *testing.T) {
			if !clickhouse_tests.CheckMinServerServerVersion(conn, 24, 8, 0) {
				t.Skip("JSON unsupported on this server version")
			}
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_1841_mixed"))
			require.NoError(t, conn.Exec(ctx, `
				CREATE TABLE test_1841_mixed (id UInt32, product JSON)
				ENGINE = MergeTree ORDER BY id
			`))
			t.Cleanup(func() { _ = conn.Exec(ctx, "DROP TABLE IF EXISTS test_1841_mixed") })

			batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_1841_mixed (id, product)")
			require.NoError(t, err)

			obj := struct {
				Name string
				ID   int32
			}{"kavi", 3}
			require.NoError(t, batch.Append(uint32(1), obj))

			err = batch.Append(uint32(2), jsonText)
			require.Error(t, err,
				"mixing object and string modes in the same block must return an error — "+
					"PR #1850 §3; previously the second value was silently lost")
			require.Contains(t, err.Error(), "serialization",
				"error must explain the mode-conflict")
		})

		// 4. Columnar bulk insert of []*string with a nil element round-trips.
		// Locks the regression fixed alongside this test: previously the bulk
		// path stored "" (server rejects with code 117) or panicked on nil
		// *json.RawMessage; the null mask returned to the Nullable wrapper
		// also has to be the correct length so the bitmap matches the data.
		t.Run("bulk Column.Append([]*string) with nil round-trips", func(t *testing.T) {
			if !clickhouse_tests.CheckMinServerServerVersion(conn, 25, 2, 0) {
				t.Skip("Nullable(JSON) requires server 25.2+")
			}
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_1841_bulk"))
			require.NoError(t, conn.Exec(ctx, `
				CREATE TABLE test_1841_bulk (id UInt32, product Nullable(JSON))
				ENGINE = MergeTree ORDER BY id
			`))
			t.Cleanup(func() { _ = conn.Exec(ctx, "DROP TABLE IF EXISTS test_1841_bulk") })

			batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_1841_bulk (id, product)")
			require.NoError(t, err)

			a, b := jsonText, `{"x":2}`
			require.NoError(t, batch.Column(0).Append([]uint32{1, 2, 3}))
			require.NoError(t, batch.Column(1).Append([]*string{&a, nil, &b}),
				"bulk Append must accept []*string with nil elements")
			require.NoError(t, batch.Send())

			rows, err := conn.Query(ctx, "SELECT id, toString(product) FROM test_1841_bulk ORDER BY id")
			require.NoError(t, err)
			defer rows.Close()

			want := map[uint32]*string{1: &a, 2: nil, 3: &b}
			seen := 0
			for rows.Next() {
				var id uint32
				var s *string
				require.NoError(t, rows.Scan(&id, &s))
				expected := want[id]
				if expected == nil {
					require.Nil(t, s, "row id=%d should be NULL on the wire (null mask must mark it)", id)
				} else {
					require.NotNil(t, s, "row id=%d must not be NULL", id)
					require.Equal(t, *expected, *s, "row id=%d content", id)
				}
				seen++
			}
			require.NoError(t, rows.Err())
			require.Equal(t, 3, seen)
		})

		// 5. After an all-null batch, Reset() must clear the latched
		// serialization mode so a follow-up batch of struct/map values
		// is not rejected with a mode-conflict error. Pre-fix: the all-null
		// batch latched String at WriteStatePrefix and Reset() left
		// serializationVersion at String, breaking the next batch.
		t.Run("Reset clears mode after all-null batch", func(t *testing.T) {
			if !clickhouse_tests.CheckMinServerServerVersion(conn, 25, 2, 0) {
				t.Skip("Nullable(JSON) requires server 25.2+")
			}
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_1841_reset"))
			require.NoError(t, conn.Exec(ctx, `
				CREATE TABLE test_1841_reset (id UInt32, product Nullable(JSON))
				ENGINE = MergeTree ORDER BY id
			`))
			t.Cleanup(func() { _ = conn.Exec(ctx, "DROP TABLE IF EXISTS test_1841_reset") })

			// First batch: only nulls — latches String at WriteStatePrefix.
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_1841_reset (id, product)")
			require.NoError(t, err)
			require.NoError(t, batch.Append(uint32(1), nil))
			require.NoError(t, batch.Send())

			// Second batch: a struct value — must succeed because Reset()
			// clears serializationVersion between sends.
			batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_1841_reset (id, product)")
			require.NoError(t, err)
			obj := struct {
				Name string
				Qty  int32
			}{"book", 7}
			require.NoError(t, batch.Append(uint32(2), obj),
				"after an all-null batch, the next batch must accept struct values — PR #1850 review fix")
			require.NoError(t, batch.Send())

			var nulls, total uint64
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT countIf(product IS NULL), count() FROM test_1841_reset").Scan(&nulls, &total))
			require.EqualValues(t, 1, nulls)
			require.EqualValues(t, 2, total)
		})
	})
}
