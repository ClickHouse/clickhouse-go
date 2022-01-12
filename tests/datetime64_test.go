package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestDateTime64(t *testing.T) {
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
		if err := checkMinServerVersion(conn, 20, 3); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE TABLE test_datetime64 (
				    Col1 DateTime64(3)
				  , Col2 DateTime64(9, 'Europe/Moscow')
				  , Col3 DateTime64(0, 'Europe/London')
			) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_datetime64"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime64"); assert.NoError(t, err) {
					var (
						datetime1 = time.Now().Truncate(time.Millisecond)
						datetime2 = time.Now().Truncate(time.Nanosecond)
						datetime3 = time.Now().Truncate(time.Second)
					)
					if err := batch.Append(datetime1, datetime2, datetime3); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1 time.Time
								col2 time.Time
								col3 time.Time
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_datetime64").Scan(&col1, &col2, &col3); assert.NoError(t, err) {
								assert.Equal(t, datetime1, col1)
								assert.Equal(t, datetime2.UnixNano(), col2.UnixNano())
								assert.Equal(t, datetime3.UnixNano(), col3.UnixNano())
								if assert.Equal(t, "Europe/Moscow", col2.Location().String()) {
									assert.Equal(t, "Europe/London", col3.Location().String())
								}
							}
						}
					}
				}
			}
		}
	}
}

func TestNullableDateTime64(t *testing.T) {
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
		if err := checkMinServerVersion(conn, 20, 3); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE TABLE test_datetime64 (
				    Col1      DateTime64(3)
				  , Col1_Null Nullable(DateTime64(3))
				  , Col2      DateTime64(9, 'Europe/Moscow')
				  , Col2_Null Nullable(DateTime64(9, 'Europe/Moscow'))
				  , Col3      DateTime64(0, 'Europe/London')
				  , Col3_Null Nullable(DateTime64(0, 'Europe/London'))
			) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_datetime64"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime64"); assert.NoError(t, err) {
					var (
						datetime1 = time.Now().Truncate(time.Millisecond)
						datetime2 = time.Now().Truncate(time.Nanosecond)
						datetime3 = time.Now().Truncate(time.Second)
					)
					if err := batch.Append(datetime1, datetime1, datetime2, datetime2, datetime3, datetime3); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1     time.Time
								col1Null *time.Time
								col2     time.Time
								col2Null *time.Time
								col3     time.Time
								col3Null *time.Time
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_datetime64").Scan(
								&col1, &col1Null,
								&col2, &col2Null,
								&col3, &col3Null,
							); assert.NoError(t, err) {
								assert.Equal(t, datetime1, col1)
								assert.Equal(t, datetime1, *col1Null)
								assert.Equal(t, datetime2.UnixNano(), col2.UnixNano())
								assert.Equal(t, datetime2.UnixNano(), col2Null.UnixNano())
								assert.Equal(t, datetime3.UnixNano(), col3.UnixNano())
								assert.Equal(t, datetime3.UnixNano(), col3Null.UnixNano())
							}
						}
					}
				}
				if err := conn.Exec(ctx, "TRUNCATE TABLE test_datetime64"); !assert.NoError(t, err) {
					return
				}
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime64"); assert.NoError(t, err) {
					var (
						datetime1 = time.Now().Truncate(time.Millisecond)
						datetime2 = time.Now().Truncate(time.Nanosecond)
						datetime3 = time.Now().Truncate(time.Second)
					)
					if err := batch.Append(datetime1, nil, datetime2, nil, datetime3, nil); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1     time.Time
								col1Null *time.Time
								col2     time.Time
								col2Null *time.Time
								col3     time.Time
								col3Null *time.Time
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_datetime64").Scan(
								&col1, &col1Null,
								&col2, &col2Null,
								&col3, &col3Null,
							); assert.NoError(t, err) {
								if assert.Nil(t, col1Null) {
									assert.Equal(t, datetime1, col1)
								}
								if assert.Nil(t, col2Null) {
									if assert.Equal(t, "Europe/Moscow", col2.Location().String()) {
										assert.Equal(t, datetime2.UnixNano(), col2.UnixNano())

									}
								}
								if assert.Nil(t, col3Null) {
									if assert.Equal(t, "Europe/London", col3.Location().String()) {
										assert.Equal(t, datetime3.UnixNano(), col3.UnixNano())
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
