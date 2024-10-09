package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"testing"
)

type T1365OrderedMap int

func (t *T1365OrderedMap) Put(k any, v any) {
	if k == "K" && v == "V" {
		*t = 0xDEDEDEAD
	}
}
func (t *T1365OrderedMap) Iterator() column.MapIterator { return t }
func (t *T1365OrderedMap) Next() bool                   { *t++; return *t == 1 }
func (t *T1365OrderedMap) Key() any                     { return "K" }
func (t *T1365OrderedMap) Value() any                   { return "V" }

func TestIssue1365(t *testing.T) {
	ctx := context.Background()

	conn, err := tests.GetConnection("issues", nil, nil, nil)
	require.NoError(t, err)
	defer conn.Close()

	const ddl = `
		CREATE TABLE test_1365 (
				Col1 Array(Map(String,String))
		) Engine MergeTree() ORDER BY tuple()
		`
	err = conn.Exec(ctx, ddl)
	require.NoError(t, err)
	defer conn.Exec(ctx, "DROP TABLE test_1365")

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_1365")
	require.NoError(t, err)

	var writeMaps []column.IterableOrderedMap
	writeMaps = append(writeMaps, new(T1365OrderedMap))
	writeMaps = append(writeMaps, new(T1365OrderedMap))

	err = batch.Append(writeMaps)
	require.NoError(t, err)

	err = batch.Send()
	require.NoError(t, err)

	rows, err := conn.Query(ctx, "SELECT * FROM test_1365")
	require.NoError(t, err)

	require.True(t, rows.Next())

	//var readMaps []*T1365OrderedMap
	//
	//err = rows.Scan(&readMaps)
	//require.NoError(t, err)
}
