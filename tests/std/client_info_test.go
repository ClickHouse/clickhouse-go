package std

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"runtime"
	"strconv"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientInfo(t *testing.T) {
	expectedClientProduct := fmt.Sprintf(
		"%s/%d.%d.%d (database/sql; lv:go/%s; os:%s)",
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
		additionalOpts     url.Values
	}{
		"no additional products": {
			// e.g. clickhouse-go/2.5.1 (database/sql; lv:go/1.19.3; os:darwin)
			expectedClientProduct,
			context.Background(),
			nil,
		},
		"one additional product": {
			// e.g. tests/dev clickhouse-go/2.5.1 (database/sql; lv:go/1.19.3; os:darwin)
			fmt.Sprintf("tests/dev %s", expectedClientProduct),
			context.Background(),
			url.Values{
				"client_info_product": []string{"tests/dev"},
			},
		},
		"two additional products": {
			// e.g. product/version tests/dev clickhouse-go/2.5.1 (database/sql; lv:go/1.19.3; os:darwin)
			fmt.Sprintf("product/version tests/dev %s", expectedClientProduct),
			context.Background(),
			url.Values{
				"client_info_product": []string{"product/version,tests/dev"},
			},
		},
		"additional product from context": {
			// e.g. ctxProduct/1.2.3 clickhouse-go/2.41.0 (database/sql; ctxComment; lv:go/1.25.5 X:nodwarf5; os:linux)
			fmt.Sprintf(
				"ctxProduct/1.2.3 %s/%d.%d.%d (database/sql; ctxComment; lv:go/%s; os:%s)",
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
			nil,
		},
	}

	dsns := []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for _, protocol := range dsns {
		t.Run(fmt.Sprintf("%s protocol", protocol.String()), func(t *testing.T) {
			for name, testCase := range testCases {
				t.Run(name, func(t *testing.T) {
					conn, err := GetStdDSNConnection(protocol, useSSL, testCase.additionalOpts)
					require.NoError(t, err)

					actualClientInfo := getConnectedClientInfo(t, conn, testCase.ctx)
					assert.Equal(t, testCase.expectedClientInfo, actualClientInfo)
				})
			}
		})
	}
}

func getConnectedClientInfo(t *testing.T, conn *sql.DB, ctx context.Context) string {
	var queryID string
	row := conn.QueryRowContext(ctx, "SELECT queryID()")
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&queryID))

	_, err := conn.ExecContext(ctx, "SYSTEM FLUSH LOGS")
	require.NoError(t, err)

	var clientName string
	row = conn.QueryRowContext(ctx, fmt.Sprintf("SELECT IF(interface = 2, http_user_agent, client_name) as client_name FROM system.query_log WHERE query_id = '%s'", queryID))
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&clientName))

	return clientName
}
