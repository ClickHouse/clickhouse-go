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

func Test2066(t *testing.T) {
	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprint(protocol), func(t *testing.T) {
			conn, err := tests.GetConnection(testSet, t, protocol, nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			if !tests.CheckMinServerServerVersion(conn, 25, 11, 0) {
				t.Skip("send_profile_events requires ClickHouse 25.11 or later")
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			t.Cleanup(cancel)
			const query = "SELECT toUInt8(getSetting('send_profile_events')), toUInt64(getSetting('max_threads'))"

			t.Run("parent_and_sibling", func(t *testing.T) {
				settings := clickhouse.Settings{"max_threads": 2, "send_profile_events": 1}
				parent := clickhouse.Context(ctx, clickhouse.WithSettings(settings))
				sibling := clickhouse.Context(parent)
				child := clickhouse.Context(parent, clickhouse.WithoutProfileEvents())
				for i, queryCtx := range []context.Context{child, parent, sibling} {
					var enabled uint8
					var threads uint64
					require.NoError(t, conn.QueryRow(queryCtx, query).Scan(&enabled, &threads))
					want := uint8(1)
					if i == 0 {
						want = 0
					}
					require.Equal(t, want, enabled)
					require.Equal(t, uint64(2), threads)
				}
				require.Equal(t, clickhouse.Settings{"max_threads": 2, "send_profile_events": 1}, settings)
			})

			t.Run("concurrent_queries", func(t *testing.T) {
				parent := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
					"max_threads": 2, "send_profile_events": 1,
				}))
				for i := range 8 {
					t.Run(fmt.Sprintf("worker_%d", i), func(t *testing.T) {
						t.Parallel()
						want := uint8(i % 2)
						for range 8 {
							queryCtx := parent
							if want == 0 {
								queryCtx = clickhouse.Context(parent, clickhouse.WithoutProfileEvents())
							}
							var enabled uint8
							var threads uint64
							require.NoError(t, conn.QueryRow(queryCtx, query).Scan(&enabled, &threads))
							require.Equal(t, want, enabled)
							require.Equal(t, uint64(2), threads)
						}
					})
				}
			})
		})
	}
}
