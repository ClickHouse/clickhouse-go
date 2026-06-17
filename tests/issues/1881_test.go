package issues

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1881_FixedUTCOffsetTimezone verifies that DateTime / DateTime64
// columns whose timezone is a synthetic fixed offset (e.g.
// DateTime('Fixed/UTC+05:30:15')) deserialize correctly. ClickHouse emits these
// non-IANA "Fixed/UTC±HH:MM:SS" names for whole-second offsets; before the fix
// the driver forwarded them to time.LoadLocation, which errored and failed the
// entire result set with "unknown time zone Fixed/UTC+05:30:15". The bug report
// notes the failure hits both the native and database/sql surfaces, so both are
// exercised here over the native and HTTP wire protocols.
func TestIssue1881_FixedUTCOffsetTimezone(t *testing.T) {
	// 1673784000 == 2023-01-15 12:00:00 UTC. Casting that instant into a synthetic
	// fixed-offset zone shifts the wall clock by the offset while the underlying
	// unix timestamp is unchanged. The wantWall values below are what the server
	// renders for each cast (verified against ClickHouse).
	const unixSeconds = int64(1673784000)

	cases := []struct {
		name       string
		query      string
		wantWall   string
		wantOffset int
	}{
		{
			name:       "DateTime",
			query:      `SELECT CAST(toDateTime(1673784000, 'UTC'), 'DateTime(\'Fixed/UTC+05:30:15\')')`,
			wantWall:   "2023-01-15 17:30:15",
			wantOffset: 5*3600 + 30*60 + 15,
		},
		{
			name:       "DateTime64",
			query:      `SELECT CAST(toDateTime64(1673784000, 3, 'UTC'), 'DateTime64(3, \'Fixed/UTC+05:30:15\')')`,
			wantWall:   "2023-01-15 17:30:15",
			wantOffset: 5*3600 + 30*60 + 15,
		},
		{
			// Highest-precision DateTime64 exercises a different scale path.
			name:       "DateTime64_ns",
			query:      `SELECT CAST(toDateTime64(1673784000, 9, 'UTC'), 'DateTime64(9, \'Fixed/UTC+05:30:15\')')`,
			wantWall:   "2023-01-15 17:30:15",
			wantOffset: 5*3600 + 30*60 + 15,
		},
		{
			// Negative offset pins the sign handling in the offset parser.
			name:       "DateTime_negative",
			query:      `SELECT CAST(toDateTime(1673784000, 'UTC'), 'DateTime(\'Fixed/UTC-08:30:15\')')`,
			wantWall:   "2023-01-15 03:29:45",
			wantOffset: -(8*3600 + 30*60 + 15),
		},
		{
			// Nullable wrapper must forward the inner Fixed/UTC type unchanged.
			name:       "Nullable_DateTime",
			query:      `SELECT CAST(toDateTime(1673784000, 'UTC'), 'Nullable(DateTime(\'Fixed/UTC+05:30:15\'))')`,
			wantWall:   "2023-01-15 17:30:15",
			wantOffset: 5*3600 + 30*60 + 15,
		},
	}

	assertResult := func(t *testing.T, got time.Time, wantWall string, wantOffset int) {
		t.Helper()
		assert.Equal(t, wantWall, got.Format("2006-01-02 15:04:05"))
		_, offset := got.Zone()
		assert.Equal(t, wantOffset, offset)
		assert.Equal(t, unixSeconds, got.Unix())
	}

	// Native surface (driver.Conn) over both wire protocols.
	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(protocol.String(), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					var got time.Time
					require.NoError(t, conn.QueryRow(context.Background(), tc.query).Scan(&got))
					assertResult(t, got, tc.wantWall, tc.wantOffset)
				})
			}
		})
	}

	// database/sql surface (clickhouse_std.go) over both wire protocols — the bug
	// report notes the same failure reproduces here.
	t.Run("std", func(t *testing.T) {
		testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
		require.NoError(t, err)

		for _, useHTTP := range []bool{false, true} {
			proto := "native"
			if useHTTP {
				proto = "http"
			}
			t.Run(proto, func(t *testing.T) {
				opts := clickhouse_tests.ClientOptionsFromEnv(testEnv, nil, useHTTP)
				db, err := sql.Open("clickhouse", clickhouse_tests.OptionsToDSN(&opts))
				require.NoError(t, err)
				t.Cleanup(func() { db.Close() })

				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						var got time.Time
						require.NoError(t, db.QueryRow(tc.query).Scan(&got))
						assertResult(t, got, tc.wantWall, tc.wantOffset)
					})
				}
			})
		}
	})
}
