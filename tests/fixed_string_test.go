package tests

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

type BinFixedString struct {
	data [10]byte
}

func (bin *BinFixedString) MarshalBinary() ([]byte, error) {
	return bin.data[:], nil
}

func (bin *BinFixedString) UnmarshalBinary(b []byte) error {
	copy(bin.data[:], b)
	return nil
}

func TestFixedString(t *testing.T) {
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
			//Debug: true,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
		CREATE TABLE test_fixed_string (
				  Col1 FixedString(5)
				, Col2 FixedString(10)
		) Engine Memory
	`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_fixed_string"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string"); assert.NoError(t, err) {
					var (
						col1Data = make([]byte, 5)
						col2Data = &BinFixedString{}
					)
					if _, err := rand.Read(col1Data); assert.NoError(t, err) {
						if _, err := rand.Read(col2Data.data[:]); assert.NoError(t, err) {
							if err := batch.Append(col1Data, col2Data); assert.NoError(t, err) {
								if assert.NoError(t, batch.Send()) {
									var (
										col1 []byte
										col2 BinFixedString
									)
									if err := conn.QueryRow(ctx, "SELECT * FROM test_fixed_string").Scan(&col1, &col2); assert.NoError(t, err) {
										assert.Equal(t, col1Data, col1)
										assert.Equal(t, col2Data.data, col2.data)
									}
								}
							}
						}
					}
				}
			}
		}
	}
}

func TestNullableFixedString(t *testing.T) {
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
			//Debug: true,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
		CREATE TABLE test_fixed_string (
				  Col1 Nullable(FixedString(5))
				, Col2 Nullable(FixedString(10))
		) Engine Memory
	`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_fixed_string"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string"); assert.NoError(t, err) {
					var (
						col1Data = make([]byte, 5)
						col2Data = &BinFixedString{}
					)
					if _, err := rand.Read(col1Data); assert.NoError(t, err) {
						if _, err := rand.Read(col2Data.data[:]); assert.NoError(t, err) {
							if err := batch.Append(col1Data, col2Data); assert.NoError(t, err) {
								if assert.NoError(t, batch.Send()) {
									var (
										col1 []byte
										col2 BinFixedString
									)
									if err := conn.QueryRow(ctx, "SELECT * FROM test_fixed_string").Scan(&col1, &col2); assert.NoError(t, err) {
										assert.Equal(t, col1Data, col1)
										assert.Equal(t, col2Data.data, col2.data)
									}
								}
							}
						}
					}
				}
				if err := conn.Exec(ctx, "TRUNCATE TABLE test_fixed_string"); !assert.NoError(t, err) {
					return
				}
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string"); assert.NoError(t, err) {
					var col1Data = make([]byte, 5)

					if _, err := rand.Read(col1Data); assert.NoError(t, err) {

						if err := batch.Append(col1Data, nil); assert.NoError(t, err) {
							if assert.NoError(t, batch.Send()) {
								var (
									col1 *[]byte
									col2 *[]byte
								)
								if err := conn.QueryRow(ctx, "SELECT * FROM test_fixed_string").Scan(&col1, &col2); assert.NoError(t, err) {
									if assert.Nil(t, col2) {
										assert.Equal(t, col1Data, *col1)
									}
								}
							}
						}
					}
				}
			}
		}
	}
}
