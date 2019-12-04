package clickhouse_test

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/ClickHouse/clickhouse-go/lib/column"
	"github.com/ClickHouse/clickhouse-go/lib/types"
	"github.com/stretchr/testify/assert"
)

const (
	tlsName = "default_tls"
)

func Test_OpenConnectAndPing(t *testing.T) {
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		assert.NoError(t, connect.Ping())
	}
}

func Test_RegisterTLSConfig(t *testing.T) {
	tlsConfig := &tls.Config{}

	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true&tls_config="+tlsName)
	assert.NoError(t, err)
	assert.EqualError(t, connect.Ping(), "invalid tls_config - no config registered under name default_tls")

	err = clickhouse.RegisterTLSConfig(tlsName, tlsConfig)
	assert.NoError(t, err)

	connect, err = sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true&secure=false&tls_config="+tlsName)
	assert.NoError(t, err)
	assert.NoError(t, connect.Ping())

	connect, err = sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true&tls_config="+tlsName)
	assert.NoError(t, err)
	assert.EqualError(t, connect.Ping(), "tls: first record does not look like a TLS handshake")

	clickhouse.DeregisterTLSConfig(tlsName)

	connect, err = sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true&tls_config="+tlsName)
	assert.NoError(t, err)
	assert.EqualError(t, connect.Ping(), "invalid tls_config - no config registered under name default_tls")
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
				datetime DateTime,
				ipv4 IPv4,
				ipv6 IPv6,
				ipv4str FixedString(16),
				ipv6str FixedString(16)
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
				datetime,
				ipv4,
				ipv6,
				ipv4str,
				ipv6str
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
				datetime,
				ipv4,
				ipv6,
				ipv4str,
				ipv6str
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
								fmt.Sprintf("string %d", i),               // string
								"RU",                                      //fixedstring,
								time.Now(),                                //date
								time.Now(),                                //datetime
								"1.2.3.4",                                 // ipv4
								"2001:0db8:85a3:0000:0000:8a2e:0370:7334", //ipv6
								column.IP(net.ParseIP("127.0.0.1").To4()),
								column.IP(net.ParseIP("2001:0db8:85a3:0000:0000:8a2e:0370:7334")),
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
							Ipv6        column.IP
							Ipv4        column.IP
							Ipv4str     column.IP
							Ipv6str     column.IP
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
									&item.Ipv4,
									&item.Ipv6,
									&item.Ipv4str,
									&item.Ipv6str,
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
								"RU",                         //fixedstring,
								time.Now(),                   //date
								time.Now(),                   //datetime
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
						if _, err := stmt.Exec(1, "RU", types.Date(time.Date(2017, 1, 20, 0, 0, 0, 0, time.Local)), time.Date(2017, 1, 20, 13, 0, 0, 0, time.Local)); !assert.NoError(t, err) {
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
							if row := connect.QueryRow("SELECT COUNT(*) FROM clickhouse_test_select WHERE id BETWEEN ? AND ?", 0, 3); assert.NotNil(t, row) {
								var count int
								if err := row.Scan(&count); assert.NoError(t, err) {
									assert.Equal(t, int(3), count)
								}
							}
							if rows, err := connect.Query("SELECT id FROM clickhouse_test_select ORDER BY id LIMIT ?", 1); assert.NoError(t, err) {
								i := 0
								for rows.Next() {
									var (
										id int32
									)
									if err := rows.Scan(&id); assert.NoError(t, err) {
										if i == 0 {
											assert.Equal(t, id, int32(1))
										} else {
											t.Error("Should return exactly one record")
										}
									}
									i++
								}
								rows.Close()
							}
							if rows, err := connect.Query("SELECT id FROM clickhouse_test_select ORDER BY id LIMIT ?,?", 1, 2); assert.NoError(t, err) {
								i := 0
								for rows.Next() {
									var (
										id int32
									)
									if err := rows.Scan(&id); assert.NoError(t, err) {
										if i == 0 {
											assert.Equal(t, id, int32(2))
										} else if i == 1 {
											assert.Equal(t, id, int32(3))
										} else {
											t.Error("Should return exactly two records")
										}
									}
									i++
								}
								rows.Close()
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
				enum16   Array(Enum16('c' = 1, 'd' = 2)),
				ipv4 Array(IPv4),
				ipv6 Array(IPv6)
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
				enum16,
				ipv4,
				ipv6
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
				?
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
				datetime,
				ipv4,
				ipv6
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
								[]string{"RU", "EN", "DE"},
								[]time.Time{time.Now(), time.Now()},
								[]time.Time{time.Now(), time.Now()},
								[]string{"a", "b"},
								[]string{"c", "d"},
								[]string{"127.0.0.1", "1.2.3.4"},
								[]string{"2001:0db8:85a3:0000:0000:8a2e:0370:7334"},
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
								[]string{"UA", "GB"},
								[]time.Time{time.Now(), time.Now(), time.Now(), time.Now()},
								[]time.Time{time.Now(), time.Now()},
								[]string{"a", "b"},
								[]string{"c", "d"},
								[]string{"127.0.0.1", "1.2.3.4"},
								[]string{"2001:0db8:85a3:0000:0000:8a2e:0370:7334"},
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
							Ipv4        []column.IP
							Ipv6        []column.IP
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
									&item.Ipv4,
									&item.Ipv6,
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
								t.Logf("Ipv4=%v, Ipv6=%v",
									item.Ipv4,
									item.Ipv6,
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
							if _, err := stmt.Exec(types.UUID("123e4567-e89b-12d3-a456-426655440000"), "123e4567-e89b-12d3-a456-426655440000"); !assert.NoError(t, err) {
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
								uuid        types.UUID
								uuidStr     string
								builtinUUID string
							)
							if err := rows.Scan(&uuid, &uuidStr, &builtinUUID); assert.NoError(t, err) {
								if assert.Equal(t, "123e4567-e89b-12d3-a456-426655440000", uuidStr) {
									assert.Equal(t, types.UUID("123e4567-e89b-12d3-a456-426655440000"), uuid)
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
				OldIPv4 FixedString(16),
				OldIPv6 FixedString(16),
				IPv4    IPv4,
				IPv6    IPv6
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
						if stmt, err := tx.Prepare("INSERT INTO clickhouse_test_ip VALUES(?, ?, ?, ?)"); assert.NoError(t, err) {
							if _, err := stmt.Exec(column.IP(ipv4), column.IP(ipv6), ipv4, ipv6); !assert.NoError(t, err) {
								t.Fatal(err)
							}
						}
						if err := tx.Commit(); !assert.NoError(t, err) {
							t.Fatal(err)
						}
					}
					if rows, err := connect.Query("SELECT OldIPv4, OldIPv6, IPv4, IPv6 FROM clickhouse_test_ip"); assert.NoError(t, err) {
						if assert.True(t, rows.Next()) {
							var (
								oldIPv4, oldIPv6 column.IP
								v4, v6           net.IP
							)
							if err := rows.Scan(&oldIPv4, &oldIPv6, &v4, &v6); assert.NoError(t, err) {
								assert.Equal(t, net.IP(oldIPv4), ipv4)
								assert.Equal(t, net.IP(oldIPv6), ipv6)
								assert.Equal(t, v4, ipv4)
								assert.Equal(t, v6, ipv6)
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
			if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(2)"); assert.NotNil(t, row) {
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

func Test_Ping_Context_Timeout(t *testing.T) {
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
			defer cancel()
			if err := connect.PingContext(ctx); assert.Error(t, err) {
				assert.Equal(t, context.DeadlineExceeded, err)
			}
		}
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
			defer cancel()
			if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(2)"); assert.NotNil(t, row) {
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
			if row := connect.QueryRow("SELECT 1, sleep(2)"); assert.NotNil(t, row) {
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

func Test_InArray(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_in_array (
				Value String
			) Engine=Memory
		`
		dml = `
			INSERT INTO clickhouse_test_in_array (Value) VALUES (?)
		`
		query = `
			SELECT
				groupArray(Value)
			FROM clickhouse_test_in_array WHERE Value IN(?)
		`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_in_array"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						for _, v := range []string{"A", "B", "C"} {
							_, err = stmt.Exec(v)
							if !assert.NoError(t, err) {
								return
							}
						}
					} else {
						return
					}
					if assert.NoError(t, tx.Commit()) {
						var value []string
						if err := connect.QueryRow(query, []string{"A", "C"}).Scan(&value); assert.NoError(t, err) {
							if !assert.NoError(t, err) {
								return
							}
						}
						assert.Equal(t, []string{"A", "C"}, value)

					}
				}
			}
		}
	}
}

func TestArrayArrayT(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_array_array_t (
				String Array(Array(String))
				, String2 Array(String)
				, Int32 Array(Int32)
			) Engine=Memory
		`
		dml = `
			INSERT INTO clickhouse_test_array_array_t (String, String2, Int32) VALUES (?)
		`
		query = `
			SELECT
				String
			FROM clickhouse_test_array_array_t
		`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_array_array_t"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						_, err = stmt.Exec([][]string{[]string{"A"}, []string{"B"}, []string{"C"}}, []string{"X", "Y"}, []int32{1, 2, 3})
						if !assert.NoError(t, err) {
							return
						}
						_, err = stmt.Exec([][]string{[]string{"AA"}, []string{"BB"}, []string{"C4C"}}, []string{"XX", "YY"}, []int32{4, 5, 6})
						if !assert.NoError(t, err) {
							return
						}
						_, err = stmt.Exec(
							[][][]byte{
								[][]byte{[]byte("AA")},
								[][]byte{[]byte("BB")},
								[][]byte{[]byte("C4C")},
							},
							[][]byte{[]byte("XX"), []byte("YY")},
							[]int32{4, 5, 6},
						)
						if !assert.NoError(t, err) {
							return
						}
					} else {
						return
					}
					if assert.NoError(t, tx.Commit()) {
						/*	var value []string
							if err := connect.QueryRow(query).Scan(&value); assert.NoError(t, err) {
								if !assert.NoError(t, err) {
									return
								}
							}
							assert.Equal(t, []string{"A", "C"}, value)
						*/
					}
				}
			}
		}
	}
}

func Test_LikeQuery(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_like (
				firstName String,
                lastName String
			) Engine=Memory
		`
		dml = `
			INSERT INTO clickhouse_test_like (
  				firstName,
				lastName
			) VALUES (
				?,
                ?
			)
		`
		query = `
			SELECT
				firstName,
                lastName
			FROM clickhouse_test_like
			WHERE firstName LIKE ? and lastName LIKE ?
		`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_like"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						var names = []struct {
							First string
							Last  string
						}{
							{First: "JeanPierre", Last: "Baltasar"}, {First: "DonPierre", Last: "Baltasar"},
						}
						for i := range names {
							_, err = stmt.Exec(
								names[i].First,
								names[i].Last,
							)
							if !assert.NoError(t, err) {
								return
							}
						}
					}
					if assert.NoError(t, tx.Commit()) {
						var tests = []struct {
							Param1        string
							Param2        string
							ExpectedFirst string
							ExpectedLast  string
						}{
							{
								Param1:        "Don%",
								Param2:        "%lta%",
								ExpectedFirst: "DonPierre",
								ExpectedLast:  "Baltasar",
							},
							{
								Param1:        "%eanP%",
								Param2:        "%asar",
								ExpectedFirst: "JeanPierre",
								ExpectedLast:  "Baltasar",
							},
							{
								Param1:        "Don",
								Param2:        "%asar",
								ExpectedFirst: "",
								ExpectedLast:  "",
							},
							{
								Param1:        "Jean%",
								Param2:        "%",
								ExpectedFirst: "JeanPierre",
								ExpectedLast:  "Baltasar",
							},
							{
								Param1:        "%",
								Param2:        "Baptiste",
								ExpectedFirst: "",
								ExpectedLast:  "",
							},
						}

						for _, test := range tests {
							var result struct {
								FirstName string
								LastName  string
							}
							if rows, err := connect.Query(query, test.Param1, test.Param2); assert.NoError(t, err) {

								for rows.Next() {
									err := rows.Scan(
										&result.FirstName,
										&result.LastName,
									)
									if !assert.NoError(t, err) {
										return
									}
								}
								assert.Equal(t, test.ExpectedFirst, result.FirstName)
								assert.Equal(t, test.ExpectedLast, result.LastName)
							}
						}
					}
				}
			}
		}
	}
}
