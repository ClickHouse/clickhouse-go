package column_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column/orderedmap"
)

type mapSQLScanner struct {
	got map[string]string
}

func (s *mapSQLScanner) Scan(src any) error {
	m, ok := src.(map[string]string)
	if !ok {
		return fmt.Errorf("unexpected %T", src)
	}
	s.got = m
	return nil
}

var _ sql.Scanner = (*mapSQLScanner)(nil)

func TestMapNestedDuplicateKeysOrdered(t *testing.T) {
	t.Parallel()

	t.Run("Array(Map)", func(t *testing.T) {
		t.Parallel()

		col, err := column.Type("Array(Map(String, String))").Column("c", &column.ServerContext{})
		require.NoError(t, err)

		inner := &orderedmap.Map[string, string]{}
		inner.Put("key", "X")
		inner.Put("key", "Y")
		require.NoError(t, col.AppendRow([]column.IterableOrderedMap{inner}))

		var got []orderedmap.Map[string, string]
		require.NoError(t, col.ScanRow(&got, 0))
		require.Len(t, got, 1)
		require.Len(t, got[0], 2)

		iter := got[0].Iterator()
		require.True(t, iter.Next())
		assert.Equal(t, "key", iter.Key())
		assert.Equal(t, "X", iter.Value())
		require.True(t, iter.Next())
		assert.Equal(t, "key", iter.Key())
		assert.Equal(t, "Y", iter.Value())
		assert.False(t, iter.Next())
	})

	t.Run("Map(String, Map)", func(t *testing.T) {
		t.Parallel()

		col, err := column.Type("Map(String, Map(String, String))").Column("c", &column.ServerContext{})
		require.NoError(t, err)

		inner := &orderedmap.Map[string, string]{}
		inner.Put("key", "X")
		inner.Put("key", "Y")
		outer := &orderedmap.Map[string, orderedmap.Map[string, string]]{}
		outer.Put("outer", *inner)
		require.NoError(t, col.AppendRow(outer))

		var got orderedmap.Map[string, orderedmap.Map[string, string]]
		require.NoError(t, col.ScanRow(&got, 0))
		require.Len(t, got, 1)

		iter := got.Iterator()
		require.True(t, iter.Next())
		assert.Equal(t, "outer", iter.Key())
		nested, ok := iter.Value().(orderedmap.Map[string, string])
		require.True(t, ok)
		require.Len(t, nested, 2)

		nIter := nested.Iterator()
		require.True(t, nIter.Next())
		assert.Equal(t, "key", nIter.Key())
		assert.Equal(t, "X", nIter.Value())
		require.True(t, nIter.Next())
		assert.Equal(t, "key", nIter.Key())
		assert.Equal(t, "Y", nIter.Value())
	})

	t.Run("Map with orderedmap values", func(t *testing.T) {
		t.Parallel()

		col, err := column.Type("Map(String, Map(String, String))").Column("c", &column.ServerContext{})
		require.NoError(t, err)

		inner := &orderedmap.Map[string, string]{}
		inner.Put("key", "X")
		inner.Put("key", "Y")
		outer := &orderedmap.Map[string, orderedmap.Map[string, string]]{}
		outer.Put("outer", *inner)
		require.NoError(t, col.AppendRow(outer))

		var got map[string]orderedmap.Map[string, string]
		require.NoError(t, col.ScanRow(&got, 0))
		require.Len(t, got, 1)
		require.Len(t, got["outer"], 2)
	})

	t.Run("reject incompatible Go map destinations", func(t *testing.T) {
		t.Parallel()

		col, err := column.Type("Map(String, String)").Column("c", &column.ServerContext{})
		require.NoError(t, err)

		om := &orderedmap.Map[string, string]{}
		om.Put("key", "v")
		require.NoError(t, col.AppendRow(om))

		var badKey map[int]string
		err = col.ScanRow(&badKey, 0)
		require.Error(t, err)
		var convErr *column.ColumnConverterError
		require.ErrorAs(t, err, &convErr)

		var badVal map[string]int
		err = col.ScanRow(&badVal, 0)
		require.Error(t, err)
		require.ErrorAs(t, err, &convErr)
	})

	t.Run("Array(Map) sql.Scanner element", func(t *testing.T) {
		t.Parallel()

		col, err := column.Type("Array(Map(String, String))").Column("c", &column.ServerContext{})
		require.NoError(t, err)

		require.NoError(t, col.AppendRow([]map[string]string{{"a": "1"}}))

		var dest []mapSQLScanner
		require.NoError(t, col.ScanRow(&dest, 0))
		require.Equal(t, map[string]string{"a": "1"}, dest[0].got)
	})

	t.Run("Tuple map field orderedmap", func(t *testing.T) {
		t.Parallel()

		col, err := column.Type("Tuple(Map(String, String))").Column("c", &column.ServerContext{})
		require.NoError(t, err)

		inner := &orderedmap.Map[string, string]{}
		inner.Put("key", "X")
		inner.Put("key", "Y")
		require.NoError(t, col.AppendRow([]column.IterableOrderedMap{inner}))

		var got []orderedmap.Map[string, string]
		require.NoError(t, col.ScanRow(&got, 0))
		require.Len(t, got, 1)
		require.Len(t, got[0], 2)
	})

	t.Run("Go map still collapses duplicates", func(t *testing.T) {
		t.Parallel()

		col, err := column.Type("Map(String, String)").Column("c", &column.ServerContext{})
		require.NoError(t, err)

		inner := &orderedmap.Map[string, string]{}
		inner.Put("key", "X")
		inner.Put("key", "Y")
		require.NoError(t, col.AppendRow(inner))

		var got map[string]string
		require.NoError(t, col.ScanRow(&got, 0))
		require.Equal(t, map[string]string{"key": "Y"}, got)
	})
}
