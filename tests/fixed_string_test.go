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
				  Col1 FixedString(10)
				, Col2 FixedString(10)
				, Col3 Nullable(FixedString(10))
				, Col4 Array(FixedString(10))
				, Col5 Array(Nullable(FixedString(10)))
		) Engine Memory
	`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_fixed_string"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string"); assert.NoError(t, err) {
					var (
						col1Data = "ClickHouse"
						col2Data = &BinFixedString{}
						col3Data = &col1Data
						col4Data = []string{"ClickHouse", "ClickHouse", "ClickHouse"}
						col5Data = []*string{&col1Data, nil, &col1Data}
					)
					if _, err := rand.Read(col2Data.data[:]); assert.NoError(t, err) {
						if err := batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data); assert.NoError(t, err) {
							if assert.NoError(t, batch.Send()) {
								var (
									col1 string
									col2 BinFixedString
									col3 *string
									col4 []string
									col5 []*string
								)
								if err := conn.QueryRow(ctx, "SELECT * FROM test_fixed_string").Scan(&col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
									assert.Equal(t, col1Data, col1)
									assert.Equal(t, col2Data.data, col2.data)
									assert.Equal(t, col3Data, col3)
									assert.Equal(t, col4Data, col4)
									assert.Equal(t, col5Data, col5)
								}
							}
						}
					}
				}
			}
		}
		if rows, err := conn.Query(ctx, "SELECT CAST('RU' AS FixedString(2)) FROM system.numbers_mt LIMIT 10"); assert.NoError(t, err) {
			var count int
			for rows.Next() {
				var code string
				if !assert.NoError(t, rows.Scan(&code)) || !assert.Equal(t, "RU", code) {
					return
				}
				count++
			}
			if assert.Equal(t, 10, count) && assert.NoError(t, rows.Err()) {
				assert.NoError(t, rows.Close())
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
				  Col1 Nullable(FixedString(10))
				, Col2 Nullable(FixedString(10))
		) Engine Memory
	`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_fixed_string"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string"); assert.NoError(t, err) {
					var (
						col1Data = "ClickHouse"
						col2Data = &BinFixedString{}
					)
					if _, err := rand.Read(col2Data.data[:]); assert.NoError(t, err) {
						if err := batch.Append(col1Data, col2Data); assert.NoError(t, err) {
							if assert.NoError(t, batch.Send()) {
								var (
									col1 string
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
				if err := conn.Exec(ctx, "TRUNCATE TABLE test_fixed_string"); !assert.NoError(t, err) {
					return
				}
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string"); assert.NoError(t, err) {
					var col1Data = "ClickHouse"
					if err := batch.Append(col1Data, nil); assert.NoError(t, err) {
						if assert.NoError(t, batch.Send()) {
							var (
								col1 *string
								col2 *string
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

func TestColumnarFixedString(t *testing.T) {
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
				  Col1 FixedString(10)
				, Col2 FixedString(10)
				, Col3 Nullable(FixedString(10))
				, Col4 Array(FixedString(10))
				, Col5 Array(Nullable(FixedString(10)))
		) Engine Memory
	`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_fixed_string"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string"); assert.NoError(t, err) {
					var (
						col1Data = "ClickHouse"
						col2Data = "XXXXXXXXXX"
						col3Data = &col1Data
						col4Data = []string{"ClickHouse", "ClickHouse", "ClickHouse"}
						col5Data = []*string{&col1Data, nil, &col1Data}
					)
					if err := batch.Column(0).Append([]string{
						col1Data, col1Data, col1Data, col1Data, col1Data,
					}); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(1).Append([]string{
						col2Data, col2Data, col2Data, col2Data, col2Data,
					}); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(2).Append([]*string{
						col3Data, col3Data, col3Data, col3Data, col3Data,
					}); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(3).Append([][]string{
						col4Data, col4Data, col4Data, col4Data, col4Data,
					}); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(4).Append([][]*string{
						col5Data, col5Data, col5Data, col5Data, col5Data,
					}); !assert.NoError(t, err) {
						return
					}
					if assert.NoError(t, batch.Send()) {
						var (
							col1 string
							col2 string
							col3 *string
							col4 []string
							col5 []*string
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_fixed_string LIMIT 1").Scan(&col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
							assert.Equal(t, col1Data, col1)
							assert.Equal(t, col2Data, col2)
							assert.Equal(t, col3Data, col3)
							assert.Equal(t, col4Data, col4)
							assert.Equal(t, col5Data, col5)
						}
					}
				}
			}
		}
	}
}
