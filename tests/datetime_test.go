package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestDateTime(t *testing.T) {
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
			CREATE TABLE test_datetime (
				    Col1 DateTime
				  , Col2 DateTime('Europe/Moscow')
				  , Col3 DateTime('Europe/London')
			) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_datetime"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime"); assert.NoError(t, err) {
					datetime := time.Now().Truncate(time.Second)
					if err := batch.Append(datetime, datetime, datetime); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1 time.Time
								col2 time.Time
								col3 time.Time
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_datetime").Scan(&col1, &col2, &col3); assert.NoError(t, err) {
								assert.Equal(t, datetime, col1)
								assert.Equal(t, datetime.Unix(), col2.Unix())
								assert.Equal(t, datetime.Unix(), col3.Unix())
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

func TestNullableDateTime(t *testing.T) {
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
			CREATE TABLE test_datetime (
				    Col1      DateTime
				  , Col1_Null Nullable(DateTime)
				  , Col2      DateTime('Europe/Moscow')
				  , Col2_Null Nullable(DateTime('Europe/Moscow'))
				  , Col3      DateTime('Europe/London')
				  , Col3_Null Nullable(DateTime('Europe/London'))
			) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_datetime"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime"); assert.NoError(t, err) {
					datetime := time.Now().Truncate(time.Second)
					if err := batch.Append(datetime, datetime, datetime, datetime, datetime, datetime); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1     time.Time
								col1Null *time.Time
								col2     time.Time
								col2Null *time.Time
								col3     time.Time
								col3Null *time.Time
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_datetime").Scan(
								&col1, &col1Null,
								&col2, &col2Null,
								&col3, &col3Null,
							); assert.NoError(t, err) {
								assert.Equal(t, datetime, col1)
								assert.Equal(t, datetime, *col1Null)
								assert.Equal(t, datetime.Unix(), col2.Unix())
								assert.Equal(t, datetime.Unix(), col2Null.Unix())
								assert.Equal(t, datetime.Unix(), col3.Unix())
								assert.Equal(t, datetime.Unix(), col3Null.Unix())
							}
						}
					}
				}
				if err := conn.Exec(ctx, "TRUNCATE TABLE test_datetime"); !assert.NoError(t, err) {
					return
				}
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime"); assert.NoError(t, err) {
					datetime := time.Now().Truncate(time.Second)
					if err := batch.Append(datetime, nil, datetime, nil, datetime, nil); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1     time.Time
								col1Null *time.Time
								col2     time.Time
								col2Null *time.Time
								col3     time.Time
								col3Null *time.Time
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_datetime").Scan(
								&col1, &col1Null,
								&col2, &col2Null,
								&col3, &col3Null,
							); assert.NoError(t, err) {
								if assert.Nil(t, col1Null) {
									assert.Equal(t, datetime, col1)
									assert.Equal(t, datetime.Unix(), col1.Unix())
								}
								if assert.Nil(t, col2Null) {
									if assert.Equal(t, "Europe/Moscow", col2.Location().String()) {
										assert.Equal(t, datetime.Unix(), col2.Unix())
										assert.Equal(t, datetime.Unix(), col2.Unix())
									}
								}
								if assert.Nil(t, col3Null) {
									if assert.Equal(t, "Europe/London", col3.Location().String()) {
										assert.Equal(t, datetime.Unix(), col3.Unix())
										assert.Equal(t, datetime.Unix(), col3.Unix())
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
