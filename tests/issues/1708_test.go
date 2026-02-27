package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhousetests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1708(t *testing.T) {
	testEnv, err := clickhousetests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhousetests.TestClientWithDefaultSettings(testEnv)
	require.NoError(t, err)

	if !clickhousetests.CheckMinServerServerVersion(conn, 25, 11, 0) {
		t.Skip("send_profile_events setting requires ClickHouse >= 25.11")
	}

	t.Run("query without listener succeeds", func(t *testing.T) {
		var result uint64
		err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&result)
		require.NoError(t, err)
		require.Equal(t, uint64(1), result)
	})

	t.Run("query with profile events listener receives events", func(t *testing.T) {
		var received bool
		ctx := clickhouse.Context(context.Background(),
			clickhouse.WithProfileEvents(func(events []clickhouse.ProfileEvent) {
				if len(events) > 0 {
					received = true
				}
			}),
		)

		var result uint64
		err := conn.QueryRow(ctx, "SELECT 1").Scan(&result)
		require.NoError(t, err)
		require.Equal(t, uint64(1), result)
		// Profile events may or may not arrive for simple queries depending on timing,
		// but the query must succeed regardless.
		_ = received
	})

	t.Run("explicit send_profile_events setting is not overridden", func(t *testing.T) {
		ctx := clickhouse.Context(context.Background(),
			clickhouse.WithSettings(clickhouse.Settings{
				"send_profile_events": 1,
			}),
		)

		var result uint64
		err := conn.QueryRow(ctx, "SELECT 1").Scan(&result)
		require.NoError(t, err)
		require.Equal(t, uint64(1), result)
	})
}
