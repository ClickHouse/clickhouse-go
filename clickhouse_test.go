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

func Test_Select(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_select (
				id       Int32,
				code     FixedString(2),
				date     Date,
				datetime DateTime
			) Engine=Memory
		`
		dml = `
			INSERT INTO clickhouse_test_select VALUES (?, ?, ?, ?)
		`
	)

	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_select"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						if _, err := stmt.Exec(1, "RU", clickhouse.Date(time.Date(2017, 1, 20, 0, 0, 0, 0, time.Local)), time.Date(2017, 1, 20, 13, 0, 0, 0, time.Local)); !assert.NoError(t, err) {
							return
						}
						if _, err := stmt.Exec(2, "UA", time.Date(2017, 1, 20, 0, 0, 0, 0, time.UTC), time.Date(2017, 1, 20, 14, 0, 0, 0, time.Local)); !assert.NoError(t, err) {
							return
						}
						if _, err := stmt.Exec(3, "DE", time.Date(2017, 1, 19, 0, 0, 0, 0, time.UTC), time.Date(2017, 1, 20, 14, 0, 0, 0, time.Local)); !assert.NoError(t, err) {
							return
						}
						if _, err := stmt.Exec(4, "US", time.Date(2017, 1, 19, 0, 0, 0, 0, time.UTC), time.Date(2017, 1, 20, 13, 0, 0, 0, time.Local)); !assert.NoError(t, err) {
							return
						}
						if assert.NoError(t, tx.Commit()) {
							if row := connect.QueryRow("SELECT COUNT(*) FROM clickhouse_test_select"); assert.NotNil(t, row) {
								var count int
								if err := row.Scan(&count); assert.NoError(t, err) {
									assert.Equal(t, int(4), count)
								}
							}
							if row := connect.QueryRow("SELECT COUNT(*) FROM clickhouse_test_select WHERE date = ?", time.Date(2017, 1, 20, 0, 0, 0, 0, time.UTC)); assert.NotNil(t, row) {
								var count int
								if err := row.Scan(&count); assert.NoError(t, err) {
									assert.Equal(t, int(2), count)
								}
							}
							if row := connect.QueryRow("SELECT COUNT(*) FROM clickhouse_test_select WHERE datetime = ?", time.Date(2017, 1, 20, 14, 0, 0, 0, time.Local)); assert.NotNil(t, row) {
								var count int
								if err := row.Scan(&count); assert.NoError(t, err) {
									assert.Equal(t, int(2), count)
								}
							}
							if row := connect.QueryRow("SELECT COUNT(*) FROM clickhouse_test_select WHERE id IN (?, ?, ?)", 1, 3, 4); assert.NotNil(t, row) {
								var count int
								if err := row.Scan(&count); assert.NoError(t, err) {
									assert.Equal(t, int(3), count)
								}
							}
							if row := connect.QueryRow("SELECT COUNT(*) FROM clickhouse_test_select WHERE code IN (?, ?, ?)", "US", "DE", "RU"); assert.NotNil(t, row) {
								var count int
								if err := row.Scan(&count); assert.NoError(t, err) {
									assert.Equal(t, int(3), count)
								}
							}
						}
					}
				}
			}
		}
	}
}

func Test_ArrayT(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_array (
				int8     Array(Int8),
				int16    Array(Int16),
				int32    Array(Int32),
				int64    Array(Int64),
				uint8    Array(UInt8),
				uint16   Array(UInt16),
				uint32   Array(UInt32),
				uint64   Array(UInt64),
				float32  Array(Float32),
				float64  Array(Float64),
				string   Array(String),
				fString  Array(FixedString(2)),
				date     Array(Date),
				datetime Array(DateTime)
			) Engine=Memory
		`
		dml = `
			INSERT INTO clickhouse_test_array (
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
            FROM clickhouse_test_array
        `
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_array"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						for i := 1; i <= 10; i++ {
							_, err = stmt.Exec(
								clickhouse.Array([]int8{1, 2, 3}),
								clickhouse.Array([]int16{5, 6, 7}),
								clickhouse.Array([]int32{8, 9, 10}),
								clickhouse.Array([]int64{11, 12, 13}),
								clickhouse.Array([]uint8{14, 15, 16}),
								clickhouse.Array([]uint16{17, 18, 19}),
								clickhouse.Array([]uint32{20, 21, 22}),
								clickhouse.Array([]uint64{23, 24, 25}),
								clickhouse.Array([]float32{32.1, 32.2}),
								clickhouse.Array([]float64{64.1, 64.2}),
								clickhouse.Array([]string{fmt.Sprintf("A_%d", i), "B", "C"}),
								clickhouse.ArrayFixedString(2, []string{"RU", "EN", "DE"}),
								clickhouse.ArrayDate([]time.Time{time.Now(), time.Now()}),
								clickhouse.ArrayDateTime([]time.Time{time.Now(), time.Now()}),
							)
							if !assert.NoError(t, err) {
								return
							}
							_, err = stmt.Exec(
								clickhouse.Array([]int8{100, 101, 102, 103, 104, 105}),
								clickhouse.Array([]int16{200, 201}),
								clickhouse.Array([]int32{300, 301, 302, 303}),
								clickhouse.Array([]int64{400, 401, 402}),
								clickhouse.Array([]uint8{250, 251, 252, 253, 254}),
								clickhouse.Array([]uint16{1000, 1001, 1002, 1003, 1004}),
								clickhouse.Array([]uint32{2001, 2002}),
								clickhouse.Array([]uint64{3000}),
								clickhouse.Array([]float32{1000.1, 100.1, 2000}),
								clickhouse.Array([]float64{640, 8, 650.9, 703.5, 800}),
								clickhouse.Array([]string{fmt.Sprintf("D_%d", i), "E", "F", "G"}),
								clickhouse.ArrayFixedString(2, []string{"UA", "GB"}),
								clickhouse.ArrayDate([]time.Time{time.Now(), time.Now(), time.Now(), time.Now()}),
								clickhouse.ArrayDateTime([]time.Time{time.Now(), time.Now()}),
							)
							if !assert.NoError(t, err) {
								return
							}
						}
					}
					if assert.NoError(t, tx.Commit()) {
						var item struct {
							Int8        []int8
							Int16       []int16
							Int32       []int32
							Int64       []int64
							UInt8       []uint8
							UInt16      []uint16
							UInt32      []uint32
							UInt64      []uint64
							Float32     []float32
							Float64     []float64
							String      []string
							FixedString []string
							Date        []time.Time
							DateTime    []time.Time
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
								t.Logf("Int8=%v, Int16=%v, Int32=%v, Int64=%v",
									item.Int8,
									item.Int16,
									item.Int32,
									item.Int64,
								)
								t.Logf("UInt8=%v, UInt16=%v, UInt32=%v, UInt64=%v",
									item.UInt8,
									item.UInt16,
									item.UInt32,
									item.UInt64,
								)
								t.Logf("Float32=%v, Float64=%v",
									item.Float32,
									item.Float64,
								)
								t.Logf("String=%v, FixedString=%v",
									item.String,
									item.FixedString,
								)
								t.Logf("Date=%v, DateTime=%v",
									item.Date,
									item.DateTime,
								)
							}
							assert.Equal(t, int(20), count)
						}
					}
				}
			}
		}
	}
}
func Test_Tx(t *testing.T) {
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if tx, err := connect.Begin(); assert.NoError(t, err) {
			_, err = tx.Query("SELECT 1")
			if assert.NoError(t, err) {
				if !assert.NoError(t, tx.Rollback()) {
					return
				}
			}
			_, err = tx.Query("SELECT 1")
			assert.Error(t, err)
		}
	}
}

func Test_Temporary_Table(t *testing.T) {
	const (
		ddl = `
			CREATE TEMPORARY TABLE clickhouse_test_temporary_table (
				ID UInt64
			);
		`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if tx, err := connect.Begin(); assert.NoError(t, err) {
			if _, err := tx.Exec(ddl); assert.NoError(t, err) {
				if _, err := tx.Exec("INSERT INTO clickhouse_test_temporary_table (ID) SELECT number AS ID FROM system.numbers LIMIT 10"); assert.NoError(t, err) {
					if rows, err := tx.Query("SELECT ID FROM clickhouse_test_temporary_table"); assert.NoError(t, err) {
						var count int
						for rows.Next() {
							var num int
							if err := rows.Scan(&num); !assert.NoError(t, err) {
								return
							}
							count++
						}
						if _, err = tx.Query("SELECT ID FROM clickhouse_test_temporary_table"); assert.NoError(t, err) {
							if _, err = connect.Query("SELECT ID FROM clickhouse_test_temporary_table"); assert.Error(t, err) {
								if exception, ok := err.(*clickhouse.Exception); assert.True(t, ok) {
									assert.Equal(t, int32(60), exception.Code)
								}
							}
						}
						if assert.Equal(t, int(10), count) {
							if assert.NoError(t, tx.Commit()) {
								assert.NoError(t, connect.Close())
							}
						}
					}
				}
			}
		}
	}
}
