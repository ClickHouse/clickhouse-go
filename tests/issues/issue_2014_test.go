package issues

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

// Test2014 verifies that a DateNamed value in time.Local or in an unnamed
// zone keeps its instant and sub-second part when it is bound client-side,
// and that the bound expression keeps the server/session timezone.
// See https://github.com/ClickHouse/clickhouse-go/issues/2014.
func Test2014(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)

	unnamed := time.FixedZone("", 0)
	testCases := []struct {
		name     string
		value    time.Time
		scale    clickhouse.TimeUnit
		typeName string
	}{
		{"epoch+1ms unnamed", time.UnixMilli(1).In(unnamed), clickhouse.MilliSeconds, "DateTime64(3)"},
		{"epoch+1ms local", time.UnixMilli(1).In(time.Local), clickhouse.MilliSeconds, "DateTime64(3)"},
		{"before 2001-09-09 millis", time.Date(1990, 5, 1, 12, 34, 56, 789_000_000, time.UTC).In(unnamed), clickhouse.MilliSeconds, "DateTime64(3)"},
		{"before 2001-09-09 micros", time.Date(1990, 5, 1, 12, 34, 56, 789_123_000, time.UTC).In(time.Local), clickhouse.MicroSeconds, "DateTime64(6)"},
		{"local nanos", time.Date(2022, 1, 12, 15, 0, 0, 123_456_789, time.UTC).In(time.Local), clickhouse.NanoSeconds, "DateTime64(9)"},
		{"local seconds", time.Date(2022, 1, 12, 15, 0, 0, 0, time.UTC).In(time.Local), clickhouse.Seconds, "DateTime"},
		{"epoch seconds", time.Unix(0, 0).In(unnamed), clickhouse.Seconds, "DateTime"},
	}

	// With TZ set, time.Local carries a zone name and takes the named-zone
	// path, so only check time.Local when it is the zone-less "Local".
	if time.Local.String() != "Local" {
		filtered := testCases[:0]
		for _, tc := range testCases {
			if tc.value.Location() != time.Local {
				filtered = append(filtered, tc)
			}
		}
		testCases = filtered
	}

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					var (
						got      time.Time
						typeName string
					)
					require.NoError(t, conn.QueryRow(context.Background(), "SELECT @t AS v, toTypeName(v)",
						clickhouse.DateNamed("t", tc.value, tc.scale),
					).Scan(&got, &typeName))
					require.True(t, tc.value.Equal(got), "want %s, got %s", tc.value.UTC(), got.UTC())
					require.Equal(t, tc.typeName, typeName)
				})
			}
		})

		t.Run(fmt.Sprintf("std %v", protocol), func(t *testing.T) {
			db, err := clickhouse_std_tests.GetOpenDBConnection(testSet, protocol, nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					var (
						got      time.Time
						typeName string
					)
					require.NoError(t, db.QueryRow("SELECT @t AS v, toTypeName(v)",
						clickhouse.DateNamed("t", tc.value, tc.scale),
					).Scan(&got, &typeName))
					require.True(t, tc.value.Equal(got), "want %s, got %s", tc.value.UTC(), got.UTC())
					require.Equal(t, tc.typeName, typeName)
				})
			}
		})
	}
}
