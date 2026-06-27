package tests

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

// TestNamedTupleComprehensive tests comprehensive named tuple functionality
func TestNamedTupleComprehensive(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
		ctx := context.Background()
		require.NoError(t, err)

		// https://github.com/ClickHouse/ClickHouse/pull/36544
		if !CheckMinServerServerVersion(conn, 22, 5, 0) {
			t.Skip("unsupported clickhouse version")
			return
		}

		// Test with complex named tuple
		const ddl = `
		CREATE TABLE test_named_tuple (
			Col1 Tuple(id Int64, name String, active Bool)
			, Col2 Tuple(user_id UInt32, profile Tuple(age UInt8, email String))
			, Col3 Tuple(data Array(Int32), metadata Map(String, String))
		) Engine MergeTree() ORDER BY tuple()
		`

		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_named_tuple")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))

		// Test insertion with map
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_named_tuple")
		require.NoError(t, err)

		var (
			col1Data = map[string]any{
				"id":     int64(1),
				"name":   "John Doe",
				"active": true,
			}
			col2Data = map[string]any{
				"user_id": uint32(123),
				"profile": map[string]any{
					"age":   uint8(30),
					"email": "john@example.com",
				},
			}
			col3Data = map[string]any{
				"data":     []int32{1, 2, 3, 4, 5},
				"metadata": map[string]string{"key1": "value1", "key2": "value2"},
			}
		)

		require.NoError(t, batch.Append(col1Data, col2Data, col3Data))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Send())

		// Test scanning into map
		var (
			col1 map[string]any
			col2 map[string]any
			col3 map[string]any
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_named_tuple").Scan(&col1, &col2, &col3))
		assert.Equal(t, col1Data, col1)
		assert.Equal(t, col2Data, col2)
		assert.Equal(t, col3Data, col3)

		// Test insertion with struct
		batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_named_tuple")
		require.NoError(t, err)

		type Profile struct {
			Age   uint8  `ch:"age"`
			Email string `ch:"email"`
		}

		type User struct {
			ID     int64  `ch:"id"`
			Name   string `ch:"name"`
			Active bool   `ch:"active"`
		}

		type UserWithProfile struct {
			UserID  uint32  `ch:"user_id"`
			Profile Profile `ch:"profile"`
		}

		type DataWithMetadata struct {
			Data     []int32           `ch:"data"`
			Metadata map[string]string `ch:"metadata"`
		}

		var (
			col1Struct = User{
				ID:     int64(2),
				Name:   "Jane Smith",
				Active: false,
			}
			col2Struct = UserWithProfile{
				UserID:  uint32(456),
				Profile: Profile{Age: 25, Email: "jane@example.com"},
			}
			col3Struct = DataWithMetadata{
				Data:     []int32{6, 7, 8, 9, 10},
				Metadata: map[string]string{"key3": "value3", "key4": "value4"},
			}
		)

		require.NoError(t, batch.Append(col1Struct, col2Struct, col3Struct))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Send())

		// Test scanning into struct
		var (
			col1Result User
			col2Result UserWithProfile
			col3Result DataWithMetadata
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_named_tuple WHERE Col1.id = $1", int64(2)).Scan(&col1Result, &col2Result, &col3Result))
		assert.Equal(t, col1Struct, col1Result)
		assert.Equal(t, col2Struct, col2Result)
		assert.Equal(t, col3Struct, col3Result)
	})
}

// TestNamedTupleWithNullableFields tests named tuples with nullable fields
func TestNamedTupleWithNullableFields(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
		ctx := context.Background()
		require.NoError(t, err)

		// https://github.com/ClickHouse/ClickHouse/pull/36544
		if !CheckMinServerServerVersion(conn, 22, 5, 0) {
			t.Skip("unsupported clickhouse version")
			return
		}

		const ddl = `
		CREATE TABLE test_named_tuple_nullable (
			Col1 Tuple(id Int64, name Nullable(String), age Nullable(UInt8))
		) Engine MergeTree() ORDER BY tuple()
		`

		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_named_tuple_nullable")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_named_tuple_nullable")
		require.NoError(t, err)

		// Test with nil values
		var (
			col1Data = map[string]any{
				"id":   int64(1),
				"name": nil, // Nullable String
				"age":  nil, // Nullable UInt8
			}
		)

		require.NoError(t, batch.Append(col1Data))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Send())

		// Test scanning nullable fields
		var col1 map[string]any
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_named_tuple_nullable").Scan(&col1))
		assert.Equal(t, int64(1), col1["id"])
		assert.Nil(t, col1["name"])
		assert.Nil(t, col1["age"])
	})
}
