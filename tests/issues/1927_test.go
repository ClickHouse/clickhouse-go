package issues

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column/orderedmap"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1927_DateQueryParameters covers the Date half of #1927: a
// time.Time sent through Named or DateNamed for a `{p:Date}` parameter was
// rendered as epoch seconds, and the server's date reader consumes at most
// YYYYMMDD from a numeric value, so the parameter was rejected with
// BAD_QUERY_PARAMETER. Date and Date32 parameters now send the calendar date,
// while the DateTime family keeps epoch seconds so the instant stays
// independent of the server's timezone.
func TestIssue1927_DateQueryParameters(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		ctx := context.Background()
		conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })

		if !clickhouse_tests.CheckMinServerServerVersion(conn, 22, 8, 0) {
			t.Skip("server-side query parameters require ClickHouse 22.8+")
		}

		d := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)

		tokyo := time.FixedZone("Asia/Tokyo", 9*3600)

		t.Run("Named Date", func(t *testing.T) {
			var got time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {d:Date}",
				clickhouse.Named("d", d),
			).Scan(&got))
			assert.Equal(t, "2026-05-15", got.Format("2006-01-02"))
		})

		t.Run("DateNamed Date", func(t *testing.T) {
			var got time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {d:Date}",
				clickhouse.DateNamed("d", d, clickhouse.Seconds),
			).Scan(&got))
			assert.Equal(t, "2026-05-15", got.Format("2006-01-02"))
		})

		t.Run("Named Date32", func(t *testing.T) {
			var got time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {d:Date32}",
				clickhouse.Named("d", d),
			).Scan(&got))
			assert.Equal(t, "2026-05-15", got.Format("2006-01-02"))
		})

		t.Run("DateNamed Date32", func(t *testing.T) {
			var got time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {d:Date32}",
				clickhouse.DateNamed("d", d, clickhouse.Seconds),
			).Scan(&got))
			assert.Equal(t, "2026-05-15", got.Format("2006-01-02"))
		})

		t.Run("Type name is case-insensitive", func(t *testing.T) {
			var got time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {d:DATE}",
				clickhouse.Named("d", d),
			).Scan(&got))
			assert.Equal(t, "2026-05-15", got.Format("2006-01-02"))
		})

		t.Run("Date in a non-UTC zone keeps its calendar day", func(t *testing.T) {
			local := time.Date(2026, 5, 15, 0, 0, 0, 0, tokyo)
			var got time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {d:Date}",
				clickhouse.Named("d", local),
			).Scan(&got))
			assert.Equal(t, "2026-05-15", got.Format("2006-01-02"))
		})

		t.Run("Nullable(Date)", func(t *testing.T) {
			var got time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {d:Nullable(Date)}",
				clickhouse.Named("d", d),
			).Scan(&got))
			assert.Equal(t, "2026-05-15", got.Format("2006-01-02"))
		})

		t.Run("Map(Date, String)", func(t *testing.T) {
			om := orderedmap.FromMapFunc(map[time.Time]string{d: "x"}, func(a, b time.Time) int { return a.Compare(b) })
			var got map[time.Time]string
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {m:Map(Date, String)}",
				clickhouse.Named("m", om),
			).Scan(&got))
			require.Len(t, got, 1)
			assert.Equal(t, "x", got[d])
		})

		t.Run("Array(Date)", func(t *testing.T) {
			var got []time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {a:Array(Date)}",
				clickhouse.Named("a", []time.Time{d}),
			).Scan(&got))
			require.Len(t, got, 1)
			assert.Equal(t, "2026-05-15", got[0].Format("2006-01-02"))
		})

		t.Run("Map(String, Date)", func(t *testing.T) {
			var got map[string]time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {m:Map(String, Date)}",
				clickhouse.Named("m", map[string]time.Time{"k": d}),
			).Scan(&got))
			require.Len(t, got, 1)
			assert.Equal(t, "2026-05-15", got["k"].Format("2006-01-02"))
		})

		// The DateTime family is unaffected: epoch seconds keep the instant.
		t.Run("Named DateTime keeps the instant", func(t *testing.T) {
			var got time.Time
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT {d:DateTime('UTC')}",
				clickhouse.Named("d", d),
			).Scan(&got))
			assert.True(t, got.Equal(d), "want %s, got %s", d.UTC(), got.UTC())
		})
	})
}
