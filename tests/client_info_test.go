package tests

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientInfo(t *testing.T) {
	expectedClientProduct := fmt.Sprintf(
		"%s/%d.%d.%d (lv:go/%s; os:%s)",
		clickhouse.ClientName,
		clickhouse.ClientVersionMajor,
		clickhouse.ClientVersionMinor,
		clickhouse.ClientVersionPatch,
		runtime.Version()[2:],
		runtime.GOOS,
	)

	testCases := map[string]struct {
		expectedClientInfo string
		ctx                context.Context
		clientInfo         clickhouse.ClientInfo
	}{
		"no additional products": {
			// e.g. clickhouse-go/2.5.1 (database/sql; lv:go/1.19.3; os:darwin)
			expectedClientProduct,
			context.Background(),
			clickhouse.ClientInfo{},
		},
		"one additional product": {
			// e.g. tests/dev clickhouse-go/2.5.1 (database/sql; lv:go/1.19.3; os:darwin)
			fmt.Sprintf("tests/dev %s", expectedClientProduct),
			context.Background(),
			clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{
						Name:    "tests",
						Version: "dev",
					},
				},
			},
		},
		"two additional products": {
			// e.g. product/version tests/dev clickhouse-go/2.5.1 (database/sql; lv:go/1.19.3; os:darwin)
			fmt.Sprintf("product/version tests/dev %s", expectedClientProduct),
			context.Background(),
			clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{
						Name:    "product",
						Version: "version",
					},
					{
						Name:    "tests",
						Version: "dev",
					},
				},
			},
		},
		"additional product from context": {
			// e.g. ctxProduct/1.2.3 clickhouse-go/2.41.0 (ctxComment; lv:go/1.25.5; os:linux)
			fmt.Sprintf(
				"ctxProduct/1.2.3 %s/%d.%d.%d (ctxComment; lv:go/%s; os:%s)",
				clickhouse.ClientName,
				clickhouse.ClientVersionMajor,
				clickhouse.ClientVersionMinor,
				clickhouse.ClientVersionPatch,
				runtime.Version()[2:],
				runtime.GOOS,
			),
			clickhouse.Context(context.Background(), clickhouse.WithClientInfo(clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{
						Name:    "ctxProduct",
						Version: "1.2.3",
					},
				},
				Comment: []string{"ctxComment"},
			})),
			clickhouse.ClientInfo{},
		},
	}

	env, err := GetTestEnvironment(testSet)
	require.NoError(t, err)

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			opts := ClientOptionsFromEnv(env, clickhouse.Settings{}, false)
			opts.ClientInfo = testCase.clientInfo

			conn, err := clickhouse.Open(&opts)
			require.NoError(t, err)

			actualClientInfo := getConnectedClientInfo(t, conn, testCase.ctx)
			assert.Equal(t, testCase.expectedClientInfo, actualClientInfo)
		})
	}
}

func getConnectedClientInfo(t *testing.T, conn driver.Conn, ctx context.Context) string {
	var queryID string
	row := conn.QueryRow(ctx, "SELECT queryID()")
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&queryID))

	err := conn.Exec(ctx, "SYSTEM FLUSH LOGS")
	require.NoError(t, err)

	var clientName string
	row = conn.QueryRow(ctx, fmt.Sprintf("SELECT IF(interface = 2, http_user_agent, client_name) as client_name FROM system.query_log WHERE query_id = '%s'", queryID))
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&clientName))

	return clientName
}
