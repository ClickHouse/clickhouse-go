package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1932_LowCardinalityNullableScanClearsDest is a regression test for
// https://github.com/ClickHouse/clickhouse-go/issues/1932: scanning a NULL from a
// LowCardinality(Nullable(T)) column must clear the destination pointer instead of
// leaving the previous row's value behind.
//
// LowCardinality tracks NULLs through key index 0 (the inner Nullable is disabled
// during parse), so LowCardinality.ScanRow — not the base column — is responsible
// for resetting the destination on NULL. It used to return without touching dest,
// so a *T reused across rows kept a stale, non-nil value after a NULL. The bug is
// invisible when each scan targets a fresh destination (see TestIssue751), so this
// test deliberately reuses the SAME pointer across an ordered [non-null, NULL] pair.
//
// String is the only base type creatable under LowCardinality(Nullable(...)) on a
// default server (numeric / Date / DateTime need allow_suspicious_low_cardinality_types,
// which the suite avoids for ClickHouse Cloud compatibility). The fix shares its
// NULL-clearing helper with Nullable.ScanRow, whose numeric/time pointer destinations
// are already exercised by the existing Nullable tests.
func TestIssue1932_LowCardinalityNullableScanClearsDest(t *testing.T) {
	conn, err := clickhouse_tests.GetConnection("issues", t, clickhouse.Native, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	if !clickhouse_tests.CheckMinServerServerVersion(conn, 19, 11, 0) {
		t.Skip("LowCardinality requires ClickHouse 19.11+")
	}

	ctx := context.Background()
	require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS issue_1932"))
	require.NoError(t, conn.Exec(ctx, "CREATE TABLE issue_1932 (c LowCardinality(Nullable(String))) ENGINE MergeTree ORDER BY tuple()"))
	t.Cleanup(func() { conn.Exec(ctx, "DROP TABLE IF EXISTS issue_1932") })

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_1932")
	require.NoError(t, err)
	require.NoError(t, batch.Append("hi")) // non-null
	require.NoError(t, batch.Append(nil))  // NULL
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM issue_1932 ORDER BY c NULLS LAST")
	require.NoError(t, err)
	defer rows.Close()

	// Reuse the SAME destination pointer across both rows: the non-null row sets it,
	// then the NULL row must reset it to nil.
	var got *string
	require.True(t, rows.Next(), "expected a non-null row")
	require.NoError(t, rows.Scan(&got))
	require.NotNil(t, got, "non-null row must populate the pointer")
	assert.Equal(t, "hi", *got)

	require.True(t, rows.Next(), "expected a NULL row")
	require.NoError(t, rows.Scan(&got))
	assert.Nil(t, got, "scanning NULL must clear the reused pointer destination")

	require.False(t, rows.Next(), "expected exactly two rows")
	require.NoError(t, rows.Err())
}
