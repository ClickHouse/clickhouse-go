package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestDate(t *testing.T) {
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
			CREATE TABLE test_date (
				    Col1 Date
			) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_date"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date"); assert.NoError(t, err) {
					date, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
					if !assert.NoError(t, err) {
						return
					}
					if err := batch.Append(date); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1 time.Time
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_date").Scan(&col1); assert.NoError(t, err) {
								if assert.Equal(t, date, col1) {
									assert.Equal(t, "UTC", col1.Location().String())
								}
							}
						}
					}
				}
			}
		}
	}
}

func TestNullableDate(t *testing.T) {
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
			CREATE TABLE test_date (
				    Col1 Date
				  , Col2 Nullable(Date)
			) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_date"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date"); assert.NoError(t, err) {
					date, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
					if !assert.NoError(t, err) {
						return
					}
					if err := batch.Append(date, date); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1 *time.Time
								col2 *time.Time
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_date").Scan(&col1, &col2); assert.NoError(t, err) {
								assert.Equal(t, date, *col1)
								assert.Equal(t, date, *col2)
							}
						}
					}
				}
				if err := conn.Exec(ctx, "TRUNCATE TABLE test_date"); !assert.NoError(t, err) {
					return
				}
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date"); assert.NoError(t, err) {
					date, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
					if !assert.NoError(t, err) {
						return
					}
					if err := batch.Append(date, nil); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1 *time.Time
								col2 *time.Time
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_date").Scan(&col1, &col2); assert.NoError(t, err) {
								if assert.Nil(t, col2) {
									assert.Equal(t, date, *col1)
									assert.Equal(t, date.Unix(), col1.Unix())
								}
							}
						}
					}
				}
			}
		}
	}
}
func TestColumnarDate(t *testing.T) {
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
		CREATE TABLE test_date (
				Col1 Date
			, Col2 Nullable(Date)
		) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_date"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date"); assert.NoError(t, err) {
					var (
						col1Data []*time.Time
						col2Data []*time.Time
					)
					date, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
					if !assert.NoError(t, err) {
						return
					}
					col1Data = append(col1Data, &date)
					col2Data = append(col2Data, nil)
					{
						batch.Column(0).Append(col1Data)
						batch.Column(1).Append(col2Data)
					}
					if assert.NoError(t, batch.Send()) {
						var (
							col1 *time.Time
							col2 *time.Time
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_date").Scan(&col1, &col2); assert.NoError(t, err) {
							if assert.Nil(t, col2) {
								assert.Equal(t, date, *col1)
							}
						}
					}
				}
			}
		}
	}
}
