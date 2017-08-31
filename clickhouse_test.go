package clickhouse_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/kshvakov/clickhouse"
	"github.com/stretchr/testify/assert"
)

func Test_OpenConnectAndPing(t *testing.T) {
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		assert.NoError(t, connect.Ping())
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
					} else {
						return
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

func Test_InsertBatch(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_insert_batch (
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
				datetime DateTime,
				arrayString Array(String)
			) Engine=Memory
		`
		dml = `
			INSERT INTO clickhouse_test_insert_batch (
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
				datetime,
				arrayString
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
				?,
				?
			)
		`
		query = `SELECT COUNT(*) FROM clickhouse_test_insert_batch`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true&block_size=11"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_insert_batch"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						for i := 1; i <= 1000; i++ {
							_, err = stmt.Exec(
								-1*i, -2*i, -4*i, -8*i, // int
								uint8(1*i), uint16(2*i), uint32(4*i), uint64(8*i), // uint
								1.32*float32(i), 1.64*float64(i), //float
								fmt.Sprintf("string %d ", i), // string
								"RU",       //fixedstring,
								time.Now(), //date
								time.Now(), //datetime
								[]string{"A", "B", "C"},
							)
							if !assert.NoError(t, err) {
								return
							}
						}
					}
					if assert.NoError(t, tx.Commit()) {
						if rows, err := connect.Query(query); assert.NoError(t, err) {
							var count int
							for rows.Next() {
								err := rows.Scan(&count)
								if !assert.NoError(t, err) {
									return
								}
							}
							assert.Equal(t, int(1000), count)
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

						{
							row1 := connect.QueryRow("SELECT COUNT(*) /* ROW1 */ FROM clickhouse_test_select WHERE date = ?", time.Date(2017, 1, 20, 0, 0, 0, 0, time.UTC))
							row2 := connect.QueryRow("SELECT COUNT(*) /* ROW2 */ FROM clickhouse_test_select WHERE datetime = ?", time.Date(2017, 1, 20, 14, 0, 0, 0, time.Local))

							if assert.NotNil(t, row2) {
								var count int
								if err := row2.Scan(&count); assert.NoError(t, err) {
									assert.Equal(t, int(2), count)
								}
							}

							if assert.NotNil(t, row1) {
								var count int
								if err := row1.Scan(&count); assert.NoError(t, err) {
									assert.Equal(t, int(2), count)
								}
							}
						}
					}
				}
			}
		}
	}
}

func Test_SimpleSelect(t *testing.T) {
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if rows, err := connect.Query("SELECT a FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a) ORDER BY a ASC"); assert.NoError(t, err) {
			defer rows.Close()
			var cnt int
			for rows.Next() {
				cnt++
				var value int
				if assert.NoError(t, rows.Scan(&value)) {
					assert.Equal(t, cnt, value)
				}
			}
			assert.Equal(t, int(3), cnt)
		}
		if row := connect.QueryRow("SELECT min(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)"); assert.NotNil(t, row) {
			var min int64
			if assert.NoError(t, row.Scan(&min)) {
				assert.Equal(t, int64(1), min)
			}
		}
		if row := connect.QueryRow("SELECT max(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)"); assert.NotNil(t, row) {
			var max int64
			if assert.NoError(t, row.Scan(&max)) {
				assert.Equal(t, int64(3), max)
			}
		}
		if row := connect.QueryRow("SELECT sum(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)"); assert.NotNil(t, row) {
			var sum int64
			if assert.NoError(t, row.Scan(&sum)) {
				assert.Equal(t, int64(6), sum)
			}
		}
		if row := connect.QueryRow("SELECT median(a) FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a)"); assert.NotNil(t, row) {
			var median float64
			if assert.NoError(t, row.Scan(&median)) {
				assert.Equal(t, float64(2), median)
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
				datetime Array(DateTime),
				enum8    Array(Enum8 ('a' = 1, 'b' = 2)),
				enum16   Array(Enum16('c' = 1, 'd' = 2))
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
				datetime,
				enum8,
				enum16
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
								[]int8{1, 2, 3},
								[]int16{5, 6, 7},
								[]int32{8, 9, 10},
								[]int64{11, 12, 13},
								clickhouse.Array([]uint8{14, 15, 16}),
								[]uint16{17, 18, 19},
								[]uint32{20, 21, 22},
								[]uint64{23, 24, 25},
								[]float32{32.1, 32.2},
								[]float64{64.1, 64.2},
								[]string{fmt.Sprintf("A_%d", i), "B", "C"},
								clickhouse.ArrayFixedString(2, []string{"RU", "EN", "DE"}),
								clickhouse.ArrayDate([]time.Time{time.Now(), time.Now()}),
								clickhouse.ArrayDateTime([]time.Time{time.Now(), time.Now()}),
								[]string{"a", "b"},
								[]string{"c", "d"},
							)
							if !assert.NoError(t, err) {
								return
							}
							_, err = stmt.Exec(
								[]int8{100, 101, 102, 103, 104, 105},
								[]int16{200, 201},
								[]int32{300, 301, 302, 303},
								[]int64{400, 401, 402},
								clickhouse.Array([]uint8{250, 251, 252, 253, 254}),
								[]uint16{1000, 1001, 1002, 1003, 1004},
								[]uint32{2001, 2002},
								[]uint64{3000},
								[]float32{1000.1, 100.1, 2000},
								[]float64{640, 8, 650.9, 703.5, 800},
								[]string{fmt.Sprintf("D_%d", i), "E", "F", "G"},
								clickhouse.ArrayFixedString(2, []string{"UA", "GB"}),
								clickhouse.ArrayDate([]time.Time{time.Now(), time.Now(), time.Now(), time.Now()}),
								clickhouse.ArrayDateTime([]time.Time{time.Now(), time.Now()}),
								[]string{"a", "b"},
								[]string{"c", "d"},
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

func Test_Insert_FixedString(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_fixed_string (
				str2  FixedString(2),
				str5  FixedString(5),
				str10 FixedString(10)
			) Engine=Memory
		`
		dml   = `INSERT INTO clickhouse_test_fixed_string VALUES (?, ?, ?)`
		query = `SELECT str2, str5, str10 FROM clickhouse_test_fixed_string`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_fixed_string"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						if _, err := stmt.Exec(strings.Repeat("a", 2), strings.Repeat("b", 5), strings.Repeat("c", 10)); assert.NoError(t, err) {
							if _, err := stmt.Exec("A", "B", "C"); assert.NoError(t, err) {
								assert.NoError(t, tx.Commit())
							}
						}
					}
				}
			}
		}
	}
}

func Test_With_Totals(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_with_totals (
				country FixedString(2)
			) Engine=Memory
		`
		dml   = `INSERT INTO clickhouse_test_with_totals (country) VALUES (?)`
		query = `
			SELECT 
				country,
				COUNT(*)
			FROM clickhouse_test_with_totals
			GROUP BY country
				WITH TOTALS
		`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_with_totals"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						if _, err := stmt.Exec("RU"); !assert.NoError(t, err) {
							return
						}
						if _, err := stmt.Exec("EN"); !assert.NoError(t, err) {
							return
						}
						if _, err := stmt.Exec("RU"); !assert.NoError(t, err) {
							return
						}
						if _, err := stmt.Exec("RU"); !assert.NoError(t, err) {
							return
						}
						if _, err := stmt.Exec("EN"); !assert.NoError(t, err) {
							return
						}
						if _, err := stmt.Exec("RU"); !assert.NoError(t, err) {
							return
						}
					}
					if assert.NoError(t, tx.Commit()) {
						var item struct {
							Country string
							Count   int64
						}
						if rows, err := connect.Query(query); assert.NoError(t, err) {
							var count int
							for rows.Next() {
								count++
								err := rows.Scan(
									&item.Country,
									&item.Count,
								)
								if !assert.NoError(t, err) {
									return
								}
								switch item.Country {
								case "RU":
									if !assert.Equal(t, int64(4), item.Count) {
										return
									}
								case "EN":
									if !assert.Equal(t, int64(2), item.Count) {
										return
									}
								}
							}

							if assert.Equal(t, int(2), count) && assert.True(t, rows.NextResultSet()) {
								var count int
								for rows.Next() {
									count++
									err := rows.Scan(
										&item.Country,
										&item.Count,
									)
									if !assert.NoError(t, err) {
										return
									}

									if assert.Equal(t, "\x00\x00", item.Country) {
										assert.Equal(t, int64(6), item.Count)
									}
								}
								assert.Equal(t, int(1), count)
							}
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
			if _, err := tx.Query("SELECT 2"); assert.Error(t, err) {
				assert.Equal(t, sql.ErrTxDone, err)
			}
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
					if rows, err := tx.Query("SELECT ID AS ID FROM clickhouse_test_temporary_table"); assert.NoError(t, err) {
						var count int
						for rows.Next() {
							var num int
							if err := rows.Scan(&num); !assert.NoError(t, err) {
								return
							}
							count++
						}
						if _, err = tx.Query("SELECT ID AS ID1 FROM clickhouse_test_temporary_table"); assert.NoError(t, err) {
							if _, err = connect.Query("SELECT ID AS ID2 FROM clickhouse_test_temporary_table"); assert.Error(t, err) {
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

func Test_Enum(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_enum (
				enum8            Enum8 ('a' = 1, 'b' = 2),
				enum16           Enum16('c' = 1, 'd' = 2),
				arr_enum8  Array(Enum8 ('a' = 1, 'b' = 2)),
				arr_enum16 Array(Enum16('c' = 1, 'd' = 2))
			) Engine=Memory
		`
		dml = `INSERT INTO clickhouse_test_enum VALUES (?, ?, ?, ?)`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_enum"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						if _, err := stmt.Exec("a", "c", []string{"a", "b"}, []string{"c", "d"}); !assert.NoError(t, err) {
							return
						}
						if _, err := stmt.Exec("b", "d", []string{"b", "a"}, []string{"d", "c"}); !assert.NoError(t, err) {
							return
						}
					}
					if err := tx.Commit(); !assert.NoError(t, err) {
						return
					}
				}
			}
		}
		if rows, err := connect.Query("SELECT enum8, enum16, arr_enum8, arr_enum16 FROM clickhouse_test_enum"); assert.NoError(t, err) {
			for rows.Next() {
				var (
					a, b string
					c, d []string
				)
				if err := rows.Scan(&a, &b, &c, &d); assert.NoError(t, err) {
					t.Log(a, b, c, d)
				}
			}
		}
	}
}

func Test_Ternary_Operator(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_ternary_operator (
				a UInt8,
				b UInt8
			) Engine=Memory
		`
		dml = `INSERT INTO clickhouse_ternary_operator VALUES (?, ?)`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_ternary_operator"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						if _, err := stmt.Exec(1, 0); !assert.NoError(t, err) {
							return
						}
					}
					if err := tx.Commit(); !assert.NoError(t, err) {
						return
					}
				}
			}
		}
		if rows, err := connect.Query("SELECT a ? '+' : '-', b ? '+' : '-' FROM clickhouse_ternary_operator WHERE a = ? AND b < ?", 1, 2); assert.NoError(t, err) {
			for rows.Next() {
				var (
					a, b string
				)
				if err := rows.Scan(&a, &b); assert.NoError(t, err) {
					assert.Equal(t, "+", a)
					assert.Equal(t, "-", b)
				}
			}
		}
		if rows, err := connect.Query("SELECT a, b FROM clickhouse_ternary_operator WHERE a = ? AND b < ?", 1, 2); assert.NoError(t, err) {
			for rows.Next() {
				var (
					a, b int
				)
				if err := rows.Scan(&a, &b); assert.NoError(t, err) {
					assert.Equal(t, 1, a)
					assert.Equal(t, 0, b)
				}
			}
		}
		if rows, err := connect.Query(`
			SELECT 
				a ? 
					'+' : '-', 
				b ? '+' : '-' ,
				a, b
			FROM clickhouse_ternary_operator 
				WHERE a = ? AND b < ? AND a IN(?,
			?
			) OR b = 0 OR b > ?`, 1, 2, 1, 100, -1); assert.NoError(t, err) {
			for rows.Next() {
				var (
					a, b string
					c, d int
				)
				if err := rows.Scan(&a, &b, &c, &d); assert.NoError(t, err) {
					assert.Equal(t, "+", a)
					assert.Equal(t, "-", b)
				}
			}
		}
	}
}

func Test_UUID(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_uuid (
				UUID    FixedString(16),
				Builtin UUID
			) Engine=Memory;
		`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if tx, err := connect.Begin(); assert.NoError(t, err) {
			if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_uuid"); assert.NoError(t, err) {
				if _, err := tx.Exec(ddl); assert.NoError(t, err) {
					if tx, err := connect.Begin(); assert.NoError(t, err) {
						if stmt, err := tx.Prepare("INSERT INTO clickhouse_test_uuid VALUES(?)"); assert.NoError(t, err) {
							if _, err := stmt.Exec(clickhouse.UUID("123e4567-e89b-12d3-a456-426655440000"), "123e4567-e89b-12d3-a456-426655440000"); !assert.NoError(t, err) {
								t.Fatal(err)
							}
						}
						if err := tx.Commit(); !assert.NoError(t, err) {
							t.Fatal(err)
						}
					}

					if rows, err := connect.Query("SELECT UUID, UUIDNumToString(UUID), Builtin FROM clickhouse_test_uuid"); assert.NoError(t, err) {
						if assert.True(t, rows.Next()) {
							var (
								uuid        clickhouse.UUID
								uuidStr     string
								builtinUUID string
							)
							if err := rows.Scan(&uuid, &uuidStr, &builtinUUID); assert.NoError(t, err) {
								if assert.Equal(t, "123e4567-e89b-12d3-a456-426655440000", uuidStr) {
									assert.Equal(t, clickhouse.UUID("123e4567-e89b-12d3-a456-426655440000"), uuid)
									assert.Equal(t, "123e4567-e89b-12d3-a456-426655440000", builtinUUID)
								}
							}
						}
					}
				}
			}
		}
	}
}

func Test_IP(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_ip (
				IPv4 FixedString(16),
				IPv6 FixedString(16)
			) Engine=Memory;
		`
	)
	var (
		ipv4 = net.ParseIP("127.0.0.1")
		ipv6 = net.ParseIP("2001:0db8:0000:0000:0000:ff00:0042:8329")
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if tx, err := connect.Begin(); assert.NoError(t, err) {
			if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_ip"); assert.NoError(t, err) {
				if _, err := tx.Exec(ddl); assert.NoError(t, err) {
					if tx, err := connect.Begin(); assert.NoError(t, err) {
						if stmt, err := tx.Prepare("INSERT INTO clickhouse_test_ip VALUES(?, ?)"); assert.NoError(t, err) {
							if _, err := stmt.Exec(clickhouse.IP(ipv4), clickhouse.IP(ipv6)); !assert.NoError(t, err) {
								t.Fatal(err)
							}
						}
						if err := tx.Commit(); !assert.NoError(t, err) {
							t.Fatal(err)
						}
					}
					if rows, err := connect.Query("SELECT IPv4, IPv6 FROM clickhouse_test_ip"); assert.NoError(t, err) {
						if assert.True(t, rows.Next()) {
							var v4, v6 clickhouse.IP
							if err := rows.Scan(&v4, &v6); assert.NoError(t, err) {
								if assert.Equal(t, ipv4, net.IP(v4)) {
									assert.Equal(t, ipv6, net.IP(v6))
								}
							}
						}
					}
				}
			}
		}
	}
}

func Test_Context_Timeout(t *testing.T) {
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
			defer cancel()
			if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(10)"); assert.NotNil(t, row) {
				var a, b int
				assert.Equal(t, driver.ErrBadConn, row.Scan(&a, &b))
			}
		}
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(0.1)"); assert.NotNil(t, row) {
				var value, value2 int
				if assert.NoError(t, row.Scan(&value, &value2)) {
					assert.Equal(t, int(1), value)
				}
			}
		}
	}
}

func Test_Timeout(t *testing.T) {
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true&read_timeout=0.2"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		{
			if row := connect.QueryRow("SELECT 1, sleep(10)"); assert.NotNil(t, row) {
				var a, b int
				assert.Equal(t, driver.ErrBadConn, row.Scan(&a, &b))
			}
		}
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(0.1)"); assert.NotNil(t, row) {
				var value, value2 int
				if assert.NoError(t, row.Scan(&value, &value2)) {
					assert.Equal(t, int(1), value)
				}
			}
		}
	}
}
