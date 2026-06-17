package issues

import (
	"context"
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
// entire result set with "unknown time zone Fixed/UTC+05:30:15".
func TestIssue1881_FixedUTCOffsetTimezone(t *testing.T) {
	// 1673784000 == 2023-01-15 12:00:00 UTC. In a +05:30:15 zone the server
	// renders this instant as 2023-01-15 17:30:15 while the underlying unix
	// timestamp is unchanged.
	const (
		unixSeconds = int64(1673784000)
		wantWall    = "2023-01-15 17:30:15"
		wantOffset  = 5*3600 + 30*60 + 15 // 19815 seconds
	)

	queries := map[string]string{
		"DateTime":   `SELECT CAST(toDateTime(1673784000, 'UTC'), 'DateTime(\'Fixed/UTC+05:30:15\')')`,
		"DateTime64": `SELECT CAST(toDateTime64(1673784000, 3, 'UTC'), 'DateTime64(3, \'Fixed/UTC+05:30:15\')')`,
	}

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(protocol.String(), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
			require.NoError(t, err)

			for name, query := range queries {
				t.Run(name, func(t *testing.T) {
					var got time.Time
					require.NoError(t, conn.QueryRow(context.Background(), query).Scan(&got))

					assert.Equal(t, wantWall, got.Format("2006-01-02 15:04:05"))
					_, offset := got.Zone()
					assert.Equal(t, wantOffset, offset)
					assert.Equal(t, unixSeconds, got.Unix())
				})
			}
		})
	}
}
