package clickhouse_test

import (
	"database/sql"
	"fmt"
	"testing"

	"time"

	"github.com/kshvakov/clickhouse"
	"github.com/stretchr/testify/assert"
)

func Test_OpenConnectAndPing(t *testing.T) {
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		assert.NoError(t, connect.Ping())
	}
	if connect, err := sql.Open("clickhouse", ""); assert.NoError(t, err) {
		assert.Error(t, connect.Ping())
	}
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:10000"); assert.NoError(t, err) {
		assert.Error(t, connect.Ping())
	}
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?username=invalid"); assert.NoError(t, err) {
		if err := connect.Ping(); assert.Error(t, err) {
			if exception, ok := err.(*clickhouse.Exception); assert.True(t, ok) {
				assert.Equal(t, int32(192), exception.Code)
			}
		}
	}
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?password=invalid"); assert.NoError(t, err) {
		if err := connect.Ping(); assert.Error(t, err) {
			if exception, ok := err.(*clickhouse.Exception); assert.True(t, ok) {
				assert.Equal(t, int32(193), exception.Code)
			}
		}
	}
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?database=invalid"); assert.NoError(t, err) {
		if err := connect.Ping(); assert.Error(t, err) {
			if exception, ok := err.(*clickhouse.Exception); assert.True(t, ok) {
				assert.Equal(t, int32(81), exception.Code)
			}
		}
	}
}

func Test_CreateTable(t *testing.T) {
	const ddl = `
        CREATE TABLE clickhouse_test_create_table (
            click_id   FixedString(64),
            click_time DateTime
        ) Engine=Memory
    `
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_create_table"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if _, err := connect.Exec(ddl); assert.Error(t, err) {
					if exception, ok := err.(*clickhouse.Exception); assert.True(t, ok) {
						assert.Equal(t, int32(57), exception.Code)
					}
				}
			}
		}
	}
}

func Test_Insert(t *testing.T) {
	const (
		ddl = `
            CREATE TABLE clickhouse_test_insert (
                int8  Int8,
                int16 Int16,
                int32 Int32,
                int64 Int64,
                uint8  UInt8,
                uint16 UInt16,
                uint32 UInt32,
                uint64 UInt64,
                float32 Float32,
                float64 Float64,
                string  String,
                fString FixedString(2),
                date    Date,
                datetime DateTime
            ) Engine=Memory
        `
		dml = `
            INSERT INTO clickhouse_test_insert (
                int8, 
                int16, 
                int32,
                int64,
                uint8, 
                uint16, 
                uint32,
                uint64,
                float32,
                float64,
                string,
                fString,
                date,
                datetime
            ) VALUES (
                ?, 
                ?, 
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?
            )
        `
		query = `
            SELECT 
                int8, 
                int16, 
                int32,
                int64,
                uint8, 
                uint16, 
                uint32,
                uint64,
                float32,
                float64,
                string,
                fString,
                date,
                datetime
            FROM clickhouse_test_insert
        `
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_insert"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						for i := 1; i <= 10; i++ {
							_, err = stmt.Exec(
								-1*i, -2*i, -4*i, -8*i, // int
								uint8(1*i), uint16(2*i), uint32(4*i), uint64(8*i), // uint
								1.32*float32(i), 1.64*float64(i), //float
								fmt.Sprintf("string %d", i), // string
								"RU",       //fixedstring,
								time.Now(), //date
								time.Now(), //datetime
							)
							if !assert.NoError(t, err) {
								return
							}
						}
					}
					if assert.NoError(t, tx.Commit()) {
						var item struct {
							Int8        int8
							Int16       int16
							Int32       int32
							Int64       int64
							UInt8       uint8
							UInt16      uint16
							UInt32      uint32
							UInt64      uint64
							Float32     float32
							Float64     float64
							String      string
							FixedString string
							Date        time.Time
							DateTime    time.Time
						}
						if rows, err := connect.Query(query); assert.NoError(t, err) {
							var count int
							for rows.Next() {
								count++
								err := rows.Scan(
									&item.Int8,
									&item.Int16,
									&item.Int32,
									&item.Int64,
									&item.UInt8,
									&item.UInt16,
									&item.UInt32,
									&item.UInt64,
									&item.Float32,
									&item.Float64,
									&item.String,
									&item.FixedString,
									&item.Date,
									&item.DateTime,
								)
								if !assert.NoError(t, err) {
									return
								}
							}
							assert.Equal(t, int(10), count)
						}
					}
				}
			}
		}

	}
}
