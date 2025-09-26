package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTimeTest(t *testing.T, protocol clickhouse.Protocol) (context.Context, func(), clickhouse.Conn) {
	ctx := context.Background()
	conn, err := GetNativeConnection(t, protocol, map[string]any{
		"enable_time_time64_type": 1,
	}, nil, nil)
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 25, 6, 0) {
		t.Skip("Time/Time64 not supported on this ClickHouse version")
	}
	return ctx, func() {}, conn
}

func TestTimeAndTime64(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		ctx, cleanup, conn := setupTimeTest(t, protocol)
		defer cleanup()
		tableName := fmt.Sprintf("test_time_types_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				t1 Time,
				t2 Time64(9),
				t3 Array(Time),
				t4 Array(Time64(9))
			) ENGINE = MergeTree() ORDER BY tuple()`, tableName)))
		// defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		// NOTE: Discuss with @spencer if year is given as 0, ch-go would still set it as 1970.
		t1 := time.Date(1970, 1, 1, 12, 34, 56, 0, time.UTC)
		t2 := time.Date(1970, 1, 1, 23, 59, 59, 123456789, time.UTC)
		t3 := []time.Time{t1, t2} // NOTE: CH server may not return arrays in same order? Discuss it with @spencer
		t4 := []time.Time{t2, t1}
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (t1, t2, t3, t4) VALUES (?, ?, ?, ?)", tableName))
		require.NoError(t, err)
		require.NoError(t, batch.Append(t1, t2, t3, t4))
		require.NoError(t, batch.Send())

		var (
			outT1 time.Time
			outT2 time.Time
			outT3 []time.Time
			outT4 []time.Time
		)
		row := conn.QueryRow(ctx, fmt.Sprintf("SELECT t1, t2, t3, t4 FROM %s", tableName))
		require.NoError(t, row.Scan(&outT1, &outT2, &outT3, &outT4))
		assert.Equal(t, t1, outT1)
		assert.Equal(t, t2, outT2)

		// NOTE: t3 is Array(Time) so it loses it's precision and only seconds are counted.
		for i, v := range t3 {
			t3[i] = v.Truncate(time.Second)
		}
		assert.Equal(t, t3, outT3)
		assert.Equal(t, t4, outT4)

		// Since both Time and Time64 doesn't support any timezone.
		// Both times should have always UTC (default) timezones.
		utc := time.UTC.String()
		t1Tz, _ := t1.Zone()
		assert.Equal(t, t1Tz, utc)
		t2Tz, _ := t2.Zone()
		assert.Equal(t, t2Tz, utc)
		for _, v := range t3 {
			tz, _ := v.Zone()
			assert.Equal(t, tz, utc)
		}
		for _, v := range t4 {
			tz, _ := v.Zone()
			assert.Equal(t, tz, utc)
		}
	})
}
