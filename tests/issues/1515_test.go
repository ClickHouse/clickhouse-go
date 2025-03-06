package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIssue1515(t *testing.T) {
	ctx := context.Background()

	conn, err := tests.GetConnection("issues", nil, nil, nil)
	require.NoError(t, err)
	defer conn.Close()

	row := conn.QueryRow(ctx, "SELECT CAST((['key', 'key2'], ['value', null]), 'Map(String, Nullable(String))')")
	require.NoError(t, row.Err())

	var resultStr map[string]*string
	err = row.Scan(&resultStr)
	require.NoError(t, err)
	require.Len(t, resultStr, 2)
	require.NotNil(t, resultStr["key"])
	require.Equal(t, "value", *resultStr["key"])
	require.Nil(t, resultStr["key2"])

	row = conn.QueryRow(ctx, "SELECT CAST((['key', 'key2'], [42, null]), 'Map(String, Nullable(Int64))')")
	require.NoError(t, row.Err())

	var resultInt map[string]*int64
	err = row.Scan(&resultInt)
	require.NoError(t, err)
	require.Len(t, resultInt, 2)
	require.NotNil(t, resultInt["key"])
	require.Equal(t, int64(42), *resultInt["key"])
	require.Nil(t, resultInt["key2"])
}
