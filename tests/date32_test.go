package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestDate32(t *testing.T) {
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
		version, err := conn.ServerVersion()
		if !assert.NoError(t, err) {
			return
		}
		if version.Version.Major < 21 || (version.Version.Major == 21 && version.Version.Minor < 9) {
			t.Skipf("server version %d.%d < 21.9", version.Version.Major, version.Version.Minor)
			return
		}
		const ddl = `
			CREATE TABLE test_date32 (
				  Col1 Date32
				, Col2 Date32
			) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_date32"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date32"); assert.NoError(t, err) {
					date1, err := time.Parse("2006-01-02 15:04:05", "2283-11-11 00:00:00")
					if !assert.NoError(t, err) {
						return
					}
					date2, err := time.Parse("2006-01-02 15:04:05", "1925-01-01 00:00:00")
					if !assert.NoError(t, err) {
						return
					}
					if err := batch.Append(date1, date2); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1 time.Time
								col2 time.Time
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_date32").Scan(&col1, &col2); assert.NoError(t, err) {
								if assert.Equal(t, date1, col1) {
									assert.Equal(t, "UTC", col1.Location().String())
								}
								if assert.Equal(t, date2, col2) {
									assert.Equal(t, "UTC", col2.Location().String())
								}
							}
						}
					}
				}
			}
		}
	}
}

func TestNullableDate32(t *testing.T) {
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
		version, err := conn.ServerVersion()
		if !assert.NoError(t, err) {
			return
		}
		if version.Version.Major < 21 || (version.Version.Major == 21 && version.Version.Minor < 9) {
			t.Skipf("server version %d.%d < 21.9", version.Version.Major, version.Version.Minor)
			return
		}
		const ddl = `
			CREATE TABLE test_date32 (
				    Col1 Date32
				  , Col2 Nullable(Date32)
			) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_date32"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date32"); assert.NoError(t, err) {
					date, err := time.Parse("2006-01-02 15:04:05", "2283-11-11 00:00:00")
					if !assert.NoError(t, err) {
						return
					}
					if err := batch.Append(date, date); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1 *time.Time
								col2 *time.Time
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_date32").Scan(&col1, &col2); assert.NoError(t, err) {
								assert.Equal(t, date, *col1)
								assert.Equal(t, date, *col2)
							}
						}
					}
				}
				if err := conn.Exec(ctx, "TRUNCATE TABLE test_date32"); !assert.NoError(t, err) {
					return
				}
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date32"); assert.NoError(t, err) {
					date, err := time.Parse("2006-01-02 15:04:05", "1925-01-01 00:00:00")
					if !assert.NoError(t, err) {
						return
					}
					if err := batch.Append(date, nil); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1 *time.Time
								col2 *time.Time
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_date32").Scan(&col1, &col2); assert.NoError(t, err) {
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
func TestColumnarDate32(t *testing.T) {
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
		version, err := conn.ServerVersion()
		if !assert.NoError(t, err) {
			return
		}
		if version.Version.Major < 21 || (version.Version.Major == 21 && version.Version.Minor < 9) {
			t.Skipf("server version %d.%d < 21.9", version.Version.Major, version.Version.Minor)
			return
		}
		const ddl = `
		CREATE TABLE test_date32 (
				Col1 Date32
			, Col2 Nullable(Date32)
		) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_date32"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date32"); assert.NoError(t, err) {
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
						if err := conn.QueryRow(ctx, "SELECT * FROM test_date32").Scan(&col1, &col2); assert.NoError(t, err) {
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
