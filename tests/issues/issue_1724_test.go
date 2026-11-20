package issues

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

const issue1724Query = "SELECT arraySort(groupArray(id)) FROM ids"

// Test1724 verifies that a query sends only the external tables of its own
// context. A child of an earlier query context must not send the earlier `ids`
// table again. Two children of one parent context must not change the tables
// of each other.
// See https://github.com/ClickHouse/clickhouse-go/issues/1724.
func Test1724(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("native/%v", protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			run1724(t, func(ctx context.Context) ([]uint64, error) {
				var ids []uint64
				err := conn.QueryRow(ctx, issue1724Query).Scan(&ids)
				return ids, err
			})
		})
		t.Run(fmt.Sprintf("std/%v", protocol), func(t *testing.T) {
			db, err := clickhouse_std_tests.GetDSNConnection(testSet, protocol, useSSL, nil)
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })

			run1724(t, func(ctx context.Context) ([]uint64, error) {
				var ids []uint64
				err := db.QueryRowContext(ctx, issue1724Query).Scan(&ids)
				return ids, err
			})
		})
	}
}

// run1724 runs the queries of Test1724. queryIDs returns the rows of the
// external table `ids` that the server got with the query.
func run1724(t *testing.T, queryIDs func(ctx context.Context) ([]uint64, error)) {
	t.Run("derived contexts", func(t *testing.T) {
		ctx := context.Background()
		for i, ids := range [][]uint64{{1, 2}, {3, 4}, {5, 6}} {
			ctx = clickhouse.Context(ctx, clickhouse.WithExternalTable(newIssue1724Table(t, "ids", ids...)))
			got, err := queryIDs(ctx)
			require.NoError(t, err)
			assert.Equal(t, ids, got, "query %d", i+1)
		}
	})

	t.Run("sibling contexts", func(t *testing.T) {
		// Use three separate calls. If WithExternalTable appends in place, the slice
		// of the parent then has spare capacity, and the two children write to the
		// same element.
		parent := context.Background()
		for _, name := range []string{"t1", "t2", "t3"} {
			parent = clickhouse.Context(parent, clickhouse.WithExternalTable(newIssue1724Table(t, name, 0)))
		}
		child1 := clickhouse.Context(parent, clickhouse.WithExternalTable(newIssue1724Table(t, "ids", 1)))
		child2 := clickhouse.Context(parent, clickhouse.WithExternalTable(newIssue1724Table(t, "ids", 2)))

		got, err := queryIDs(child1)
		require.NoError(t, err)
		assert.Equal(t, []uint64{1}, got, "child1")

		got, err = queryIDs(child2)
		require.NoError(t, err)
		assert.Equal(t, []uint64{2}, got, "child2")
	})
}

func newIssue1724Table(t *testing.T, name string, ids ...uint64) *ext.Table {
	t.Helper()
	table, err := ext.NewTable(name, ext.Column("id", "UInt64"))
	require.NoError(t, err)
	for _, id := range ids {
		require.NoError(t, table.Append(id))
	}
	return table
}
