package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestVariantDynamicPointerScan(t *testing.T) {
	columns := []struct {
		name string
		typ  string
	}{
		{name: "Variant", typ: "Variant(Int64, String)"},
		{name: "Dynamic", typ: "Dynamic"},
	}
	values := []struct {
		value any
		typ   string
	}{
		{value: nil},
		{value: int64(42), typ: "Int64"},
		{value: "hello", typ: "String"},
		{value: "", typ: "String"},
		{value: nil},
		{value: int64(-84), typ: "Int64"},
	}

	for _, column := range columns {
		t.Run(column.name, func(t *testing.T) {
			TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
				setup := setupVariantTest
				if column.name == "Dynamic" {
					setup = setupDynamicTest
				}
				conn := setup(t, protocol)
				ctx := context.Background()
				const table = "test_variant_dynamic_pointer_scan"
				ddl := fmt.Sprintf("CREATE TABLE %s (id UInt8, value %s) ENGINE = MergeTree() ORDER BY id", table, column.typ)
				require.NoError(t, conn.Exec(ctx, ddl))
				t.Cleanup(func() { require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+table)) })

				batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
				require.NoError(t, err)
				defer batch.Close()
				for i, row := range values {
					require.NoError(t, batch.Append(uint8(i), clickhouse.NewVariantWithType(row.value, row.typ)))
				}
				require.NoError(t, batch.Send())

				for _, scanStruct := range []bool{false, true} {
					name := "Scan"
					if scanStruct {
						name = "ScanStruct"
					}
					t.Run(name, func(t *testing.T) {
						rows, err := conn.Query(ctx, "SELECT value FROM "+table+" ORDER BY id")
						require.NoError(t, err)
						defer rows.Close()
						var result struct {
							Value *clickhouse.Variant `ch:"value"`
						}
						for _, row := range values {
							require.True(t, rows.Next())
							if scanStruct {
								require.NoError(t, rows.ScanStruct(&result))
							} else {
								require.NoError(t, rows.Scan(&result.Value))
							}
							require.NotNil(t, result.Value)
							assert.Equal(t, row.value, result.Value.Any())
							assert.Equal(t, row.typ, result.Value.Type())
						}
						require.False(t, rows.Next())
						require.NoError(t, rows.Err())
					})
				}
			})
		})
	}
}
