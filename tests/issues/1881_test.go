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
//
// The exhaustive offset-parsing matrix (extra precisions, boundary offsets and
// the error cases) is pinned by the fast lib/timezone unit tests. This
// end-to-end test deliberately uses a single combined round-trip per surface so
// it stays cheap in the shared integration suite.
func TestIssue1881_FixedUTCOffsetTimezone(t *testing.T) {
	// 1673784000 == 2023-01-15 12:00:00 UTC. Casting that instant into a
	// synthetic fixed-offset zone shifts the wall clock by the offset while the
	// underlying unix timestamp is unchanged. One query exercises the DateTime
	// and DateTime64 decode paths plus a negative offset in a single round-trip;
	// the want* values are what the server renders for each cast.
	const unixSeconds = int64(1673784000)
	const query = `SELECT
		CAST(toDateTime(1673784000, 'UTC'), 'DateTime(\'Fixed/UTC+05:30:15\')')           AS dt,
		CAST(toDateTime64(1673784000, 3, 'UTC'), 'DateTime64(3, \'Fixed/UTC+05:30:15\')') AS dt64,
		CAST(toDateTime(1673784000, 'UTC'), 'DateTime(\'Fixed/UTC-08:30:15\')')           AS dtNeg`

	assertRow := func(t *testing.T, dt, dt64, dtNeg time.Time) {
		t.Helper()
		for _, c := range []struct {
			label      string
			got        time.Time
			wantWall   string
			wantOffset int
		}{
			{"DateTime", dt, "2023-01-15 17:30:15", 5*3600 + 30*60 + 15},
			{"DateTime64", dt64, "2023-01-15 17:30:15", 5*3600 + 30*60 + 15},
			{"DateTime_negative", dtNeg, "2023-01-15 03:29:45", -(8*3600 + 30*60 + 15)},
		} {
			assert.Equal(t, c.wantWall, c.got.Format("2006-01-02 15:04:05"), c.label)
			_, offset := c.got.Zone()
			assert.Equal(t, c.wantOffset, offset, c.label)
			assert.Equal(t, unixSeconds, c.got.Unix(), c.label)
		}
	}

	// Native surface (driver.Conn) over both wire protocols.
	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(protocol.String(), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			var dt, dt64, dtNeg time.Time
			require.NoError(t, conn.QueryRow(context.Background(), query).Scan(&dt, &dt64, &dtNeg))
			assertRow(t, dt, dt64, dtNeg)
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

				var dt, dt64, dtNeg time.Time
				require.NoError(t, db.QueryRow(query).Scan(&dt, &dt64, &dtNeg))
				assertRow(t, dt, dt64, dtNeg)
			})
		}
	})
}
