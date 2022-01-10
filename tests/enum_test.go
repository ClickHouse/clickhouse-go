package tests

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/stretchr/testify/assert"
)

func TestEnum(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			//	Debug: true,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
			CREATE TABLE test_enum (
				  Col1 Enum  ('hello'   = 1,  'world' = 2)
				, Col2 Enum8 ('click'   = 5,  'house' = 25)
				, Col3 Enum16('default' = 10, 'value' = 50)
			) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_enum"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_enum"); assert.NoError(t, err) {
					if err := batch.Append("hello", "click", "value"); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1 string
								col2 string
								col3 string
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_enum").Scan(&col1, &col2, &col3); assert.NoError(t, err) {
								assert.Equal(t, "hello", col1)
								assert.Equal(t, "click", col2)
								assert.Equal(t, "value", col3)
							}
						}
					}
				}
			}
		}
	}
}

func TestNullableEnum(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			Debug: true,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
			CREATE TABLE test_enum (
				  Col1 Nullable(Enum  ('hello'   = 1,  'world' = 2))
				, Col2 Nullable(Enum8 ('click'   = 5,  'house' = 25))
				, Col3 Nullable(Enum16('default' = 10, 'value' = 50))
			) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_enum"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_enum"); assert.NoError(t, err) {
					if err := batch.Append("hello", "click", "value"); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1 string
								col2 string
								col3 string
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_enum").Scan(&col1, &col2, &col3); assert.NoError(t, err) {
								assert.Equal(t, "hello", col1)
								assert.Equal(t, "click", col2)
								assert.Equal(t, "value", col3)
							}
						}
					}
				}
				if err := conn.Exec(ctx, "TRUNCATE TABLE test_enum"); !assert.NoError(t, err) {
					return
				}
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_enum"); assert.NoError(t, err) {
					if err := batch.Append("hello", nil, "value"); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1 *string
								col2 *string
								col3 *string
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_enum").Scan(&col1, &col2, &col3); assert.NoError(t, err) {
								if assert.Nil(t, col2) {
									assert.Equal(t, "hello", *col1)
									assert.Equal(t, "value", *col3)
								}
							}
						}
					}
				}
			}
		}
	}
}
