package clickhouse_test

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"reflect"
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
				datetime64 DateTime64,
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
				datetime64,
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
				datetime64,
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
								fmt.Sprintf("string %d", i), // string
								"RU",                        //fixedstring,
								time.Now(),                  //date
								time.Now(),                  //datetime
								time.Now(),                  //datetime64
								"1.2.3.4",                   // ipv4
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
							DateTime64  time.Time
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
									&item.DateTime64,
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
							if rows, err := connect.Query("SELECT id FROM clickhouse_test_select ORDER BY id LIMIT ? OFFSET ?", 2, 1); assert.NoError(t, err) {
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

func Test_Select_External_Tables(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_select_external_tables (
				string1  String,
				string2  String
			) Engine=Memory
		`
		dml = `
			INSERT INTO clickhouse_test_select_external_tables (
				string1,
				string2
			) VALUES (
				?,
				?
			)
		`
		query      = `SELECT COUNT(*) FROM clickhouse_test_select_external_tables WHERE string1 IN ? AND string2 IN ? AND string1 NOT IN (SELECT c1 FROM ?)`
		queryNamed = `SELECT COUNT(*) FROM clickhouse_test_select_external_tables WHERE string1 IN @e1 AND string2 IN @e2 AND string1 NOT IN (SELECT c1 FROM @e3)`
		queryJoin  = `SELECT COUNT(*) FROM clickhouse_test_select_external_tables AS ctset JOIN ? AS ext ON ctset.string1 = ext.c1`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_select_external_tables"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						for i := 1; i <= 1000; i++ {
							_, err = stmt.Exec(
								fmt.Sprintf("string %d", i), // string1
								fmt.Sprintf("string %d", i), // string2
							)
							if !assert.NoError(t, err) {
								return
							}
						}
					}

					col, err := column.Factory("c1", "String", nil)
					if err != nil {
						t.Error(err)
						return
					}
					externalTable1 := clickhouse.ExternalTable{
						Name: "e1",
						Values: [][]driver.Value{
							{"string 1"},
							{"string 2"},
						},
						Columns: []column.Column{
							col,
						},
					}
					externalTable2 := clickhouse.ExternalTable{
						Name: "e2",
						Values: [][]driver.Value{
							{"string 1"},
							{"string 2"},
						},
						Columns: []column.Column{
							col,
						},
					}
					externalTable3 := clickhouse.ExternalTable{
						Name: "e3",
						Values: [][]driver.Value{
							{"string 1"},
						},
						Columns: []column.Column{
							col,
						},
					}
					if assert.NoError(t, tx.Commit()) {
						if rows, err := connect.Query(query, externalTable1, externalTable2, externalTable3); assert.NoError(t, err) {
							var count int
							for rows.Next() {
								err := rows.Scan(&count)
								if !assert.NoError(t, err) {
									return
								}
							}
							assert.Equal(t, 1, count)
						}
						if rows, err := connect.Query(queryNamed, sql.Named("e1", externalTable1),
							sql.Named("e2", externalTable2), sql.Named("e3", externalTable3)); assert.NoError(t, err) {
							var count int
							for rows.Next() {
								err := rows.Scan(&count)
								if !assert.NoError(t, err) {
									return
								}
							}
							assert.Equal(t, 1, count)
						}
						if rows, err := connect.Query(queryJoin, externalTable1); assert.NoError(t, err) {
							var count int
							for rows.Next() {
								err := rows.Scan(&count)
								if !assert.NoError(t, err) {
									return
								}
							}
							assert.Equal(t, 2, count)
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
				String1 Array(Array(String)),
				String2 Array(Array(Array(String))),
				Int32   Array(Array(Int32))
			) Engine=Memory
		`
		dml = `
			INSERT INTO clickhouse_test_array_array_t (String1, String2, Int32) VALUES (?)
		`
		query = `
			SELECT
				String1,
				String2,
				Int32
			FROM clickhouse_test_array_array_t
		`
	)

	items := []struct {
		String1, String2, Int32 interface{}
	}{
		{
			[][]string{
				[]string{"A"},
				[]string{"BC"},
				[]string{"DEF"},
			},
			[][][]string{
				[][]string{
					[]string{"X"},
					[]string{"Y"},
				},
				[][]string{
					[]string{"ZZ"},
				},
			},
			[][]int32{
				[]int32{1},
				[]int32{2, 3},
			},
		},
		{
			[][][]byte{
				[][]byte{[]byte("AA")},
				[][]byte{[]byte("BB")},
				[][]byte{[]byte("C4C")},
			},
			[][][][]byte{
				[][][]byte{
					[][]byte{[]byte("XX"), []byte("YY")},
				},
			},
			[][]int32{
				[]int32{4, 5, 6},
			},
		},
	}

	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_array_array_t"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						for _, item := range items {
							_, err = stmt.Exec(item.String1, item.String2, item.Int32)
							if !assert.NoError(t, err) {
								return
							}
						}

					}
					if assert.NoError(t, tx.Commit()) {
						var result struct {
							String1 [][]string
							String2 [][][]string
							Int32   [][]int32
						}

						row := connect.QueryRow(query)
						if err := row.Scan(&result.String1, &result.String2, &result.Int32); assert.NoError(t, err) {
							assert.Equal(t, items[0].String1, result.String1)
							assert.Equal(t, items[0].String2, result.String2)
							assert.Equal(t, items[0].Int32, result.Int32)
						}
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

func Test_NullableScan(t *testing.T) {
	const (
		ddl = `
 			CREATE TABLE clickhouse_test_scan_nullable (
 				int8N      Nullable(Int8),
 				int16N     Nullable(Int16),
 				int32N     Nullable(Int32),
 				int64N     Nullable(Int64),
 				uint8N     Nullable(UInt8),
 				uint16N    Nullable(UInt16),
 				uint32N    Nullable(UInt32),
 				uint64N    Nullable(UInt64),
 				float32N   Nullable(Float32),
 				float64N   Nullable(Float64),
 				string     Nullable(String)
 			) Engine=Memory;
 		`
		dml = `
 			INSERT INTO clickhouse_test_scan_nullable (
 				int8N,
 				int16N,
 				int32N,
 				int64N,
 				uint8N,
 				uint16N,
 				uint32N,
 				uint64N,
 				float32N,
 				float64N,
				string
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
 				?
 			)
 		`
		query = `
 			SELECT
				int8N,
 				int16N,
 				int32N,
 				int64N,
 				uint8N,
 				uint16N,
 				uint32N,
 				uint64N,
 				float32N,
 				float64N,
			   	string
 			FROM clickhouse_test_scan_nullable
 		`
	)

	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if tx, err := connect.Begin(); assert.NoError(t, err) {
			if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_scan_nullable"); assert.NoError(t, err) {
				if _, err := tx.Exec(ddl); assert.NoError(t, err) {
					if tx, err := connect.Begin(); assert.NoError(t, err) {
						if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
							if _, err := stmt.Exec(
								nil,
								nil,
								nil,
								nil,
								nil,
								nil,
								nil,
								nil,
								nil,
								nil,
								nil,
							); !assert.NoError(t, err) {
								t.Fatal(err)
							}
						}
						if err := tx.Commit(); !assert.NoError(t, err) {
							t.Fatal(err)
						}
					}
					if rows, err := connect.Query(query); assert.NoError(t, err) {
						if columns, err := rows.ColumnTypes(); assert.NoError(t, err) {
							values := make([]interface{}, len(columns))
							for i, c := range columns {
								values[i] = reflect.New(c.ScanType()).Interface()
							}

							for i := 0; rows.Next(); i++ {
								if err := rows.Scan(values...); assert.NoError(t, err) {
									t.Log(values)
								}
							}
						}
					}
				}
			}
		}
	}
}

func Test_Tuple(t *testing.T) {
	const (
		ddl = `
 			CREATE TABLE clickhouse_test_tuple (
 			    int8       Int8,
 				int16      Int16,
 				int32      Int32,
 				int64      Int64,
 				uint8      UInt8,
 				uint16     UInt16,
 				uint32     UInt32,
 				uint64     UInt64,
 				float32    Float32,
 				float64    Float64,
 				string     String,
 				fString    FixedString(2),
 				date       Date,
 				datetime   DateTime,
 				enum8      Enum8 ('a' = 1, 'b' = 2),
 				enum16     Enum16('c' = 1, 'd' = 2),
 				array      Array(String),
 				arrayArray Array(Array(String)),
 				int8N      Nullable(Int8),
 				int16N     Nullable(Int16),
 				int32N     Nullable(Int32),
 				int64N     Nullable(Int64),
 				uint8N     Nullable(UInt8),
 				uint16N    Nullable(UInt16),
 				uint32N    Nullable(UInt32),
 				uint64N    Nullable(UInt64),
 				float32N   Nullable(Float32),
 				float64N   Nullable(Float64),
 				stringN    Nullable(String),
 				fStringN   Nullable(FixedString(2)),
 				dateN      Nullable(Date),
 				datetimeN  Nullable(DateTime),
 				enum8N     Nullable(Enum8 ('a' = 1, 'b' = 2)),
 				enum16N    Nullable(Enum16('c' = 1, 'd' = 2))
 			) Engine=Memory;
 		`
		dml = `
 			INSERT INTO clickhouse_test_tuple (
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
 				array,
 				arrayArray,
 				int8N,
 				int16N,
 				int32N,
 				int64N,
 				uint8N,
 				uint16N,
 				uint32N,
 				uint64N,
 				float32N,
 				float64N,
 				stringN,
 				fStringN,
 				dateN,
 				datetimeN,
 				enum8N,
 				enum16N
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
 				(
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
 					array,
 				    (6.2, 'test'),
 					int8N,
 					int16N,
 					int32N,
 					int64N,
 				    uint8N,
 					uint16N,
 					uint32N,
 					uint64N,
 					float32N,
 					float64N,
 					stringN,
 					fStringN,
 					dateN,
 					datetimeN,
 					enum8N,
 					enum16N
 				)
 			FROM clickhouse_test_tuple
 		`
	)

	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if tx, err := connect.Begin(); assert.NoError(t, err) {
			if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_tuple"); assert.NoError(t, err) {
				if _, err := tx.Exec(ddl); assert.NoError(t, err) {
					if tx, err := connect.Begin(); assert.NoError(t, err) {
						if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
							for i := 0; i < 10; i++ {
								if _, err := stmt.Exec(
									8,
									16,
									32,
									64,
									18,
									116,
									132,
									165,
									1.1,
									2.2,
									"RU",
									"CN",
									time.Now(),
									time.Now(),
									"a",
									"c",
									[]string{"A", "B", "C"},
									[][]string{{"A", "B"}, {"CC", "DD", "EE"}},
									new(int8),
									16,
									new(int32),
									64,
									18,
									116,
									132,
									165,
									1.1,
									2.2,
									nil,
									"CN",
									time.Now(),
									time.Now(),
									"a",
									"c",
								); !assert.NoError(t, err) {
									t.Fatal(err)
								}
							}
						}
						if err := tx.Commit(); !assert.NoError(t, err) {
							t.Fatal(err)
						}
					}
					if rows, err := connect.Query(query); assert.NoError(t, err) {
						for i := 0; rows.Next(); i++ {
							var (
								tuple []interface{}
							)
							if err := rows.Scan(
								&tuple,
							); assert.NoError(t, err) {
								if assert.IsType(t, int8(8), tuple[0]) {
									assert.Equal(t, int8(8), tuple[0].(int8))
								}
								if assert.IsType(t, int16(16), tuple[1]) {
									assert.Equal(t, int16(16), tuple[1].(int16))
								}
								if assert.IsType(t, int32(32), tuple[2]) {
									assert.Equal(t, int32(32), tuple[2].(int32))
								}
								if assert.IsType(t, int64(64), tuple[3]) {
									assert.Equal(t, int64(64), tuple[3].(int64))
								}
								if assert.IsType(t, uint8(18), tuple[4]) {
									assert.Equal(t, uint8(18), tuple[4].(uint8))
								}
								if assert.IsType(t, uint16(116), tuple[5]) {
									assert.Equal(t, uint16(116), tuple[5].(uint16))
								}
								if assert.IsType(t, uint32(132), tuple[6]) {
									assert.Equal(t, uint32(132), tuple[6].(uint32))
								}
								if assert.IsType(t, uint64(165), tuple[7]) {
									assert.Equal(t, uint64(165), tuple[7].(uint64))
								}
								if assert.IsType(t, float32(1.1), tuple[8]) {
									assert.Equal(t, float32(1.1), tuple[8].(float32))
								}
								if assert.IsType(t, float64(2.2), tuple[9]) {
									assert.Equal(t, float64(2.2), tuple[9].(float64))
								}
								if assert.IsType(t, "RU", tuple[10]) {
									assert.Equal(t, "RU", tuple[10].(string))
								}
								if assert.IsType(t, "CN", tuple[11]) {
									assert.Equal(t, "CN", tuple[11].(string))
								}
								if assert.IsType(t, time.Now(), tuple[12]) {
									// nothing
								}
								if assert.IsType(t, time.Now(), tuple[13]) {
									// nothing
								}
								if assert.IsType(t, "a", tuple[14]) {
									assert.Equal(t, "a", tuple[14].(string))
								}
								if assert.IsType(t, "c", tuple[15]) {
									assert.Equal(t, "c", tuple[15].(string))
								}
								if assert.IsType(t, []string{"A", "B", "C"}, tuple[16]) {
									assert.Equal(t, []string{"A", "B", "C"}, tuple[16].([]string))
								}
								if assert.IsType(t, []interface{}{}, tuple[17]) {
									assert.Equal(t, []interface{}{6.2, "test"}, tuple[17].([]interface{}))
								}

								if assert.IsType(t, int8(0), tuple[18]) {
									assert.Equal(t, int8(0), tuple[18].(int8))
								}
								if assert.IsType(t, int16(16), tuple[19]) {
									assert.Equal(t, int16(16), tuple[19].(int16))
								}
								if assert.IsType(t, int32(0), tuple[20]) {
									assert.Equal(t, int32(0), tuple[20].(int32))
								}
								if assert.IsType(t, int64(64), tuple[21]) {
									assert.Equal(t, int64(64), tuple[21].(int64))
								}
								if assert.IsType(t, uint8(18), tuple[22]) {
									assert.Equal(t, uint8(18), tuple[22].(uint8))
								}
								if assert.IsType(t, uint16(116), tuple[23]) {
									assert.Equal(t, uint16(116), tuple[23].(uint16))
								}
								if assert.IsType(t, uint32(132), tuple[24]) {
									assert.Equal(t, uint32(132), tuple[24].(uint32))
								}
								if assert.IsType(t, uint64(165), tuple[25]) {
									assert.Equal(t, uint64(165), tuple[25].(uint64))
								}
								if assert.IsType(t, float32(1.1), tuple[26]) {
									assert.Equal(t, float32(1.1), tuple[26].(float32))
								}
								if assert.IsType(t, float64(2.2), tuple[27]) {
									assert.Equal(t, float64(2.2), tuple[27].(float64))
								}
								if assert.Nil(t, tuple[28]) {
									if assert.IsType(t, "CN", tuple[29]) {
										assert.Equal(t, "CN", tuple[29].(string))
									}
								}

								t.Log(tuple)
							}
						}
					}
				}
			}
		}
	}
}

func Test_ReadHistogram(t *testing.T) {
	const (
		ddl = `
 			CREATE TABLE clickhouse_test_histogram (
 			    int8       Int8,
 				int16      Int16,
 				int32      Int32,
 				int64      Int64,
 				uint8      UInt8,
 				uint16     UInt16,
 				uint32     UInt32,
 				uint64     UInt64,
 				float32    Float32,
 				float64    Float64,
 				int8N      Nullable(Int8),
 				int16N     Nullable(Int16),
 				int32N     Nullable(Int32),
 				int64N     Nullable(Int64),
 				uint8N     Nullable(UInt8),
 				uint16N    Nullable(UInt16),
 				uint32N    Nullable(UInt32),
 				uint64N    Nullable(UInt64),
 				float32N   Nullable(Float32),
 				float64N   Nullable(Float64)
 			) Engine=Memory;
 		`
		dml = `
 			INSERT INTO clickhouse_test_histogram (
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
 				int8N,
 				int16N,
 				int32N,
 				int64N,
 				uint8N,
 				uint16N,
 				uint32N,
 				uint64N,
 				float32N,
 				float64N
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
 				?,
 				?,
 				?
 			)
 		`
		query = `
 			SELECT
				histogram(5)(int8),
				histogram(5)(int16),
				histogram(5)(int32),
				histogram(5)(int64),
				histogram(5)(uint8),
				histogram(5)(uint16),
				histogram(5)(uint32),
				histogram(5)(uint64),
				histogram(5)(float32),
				histogram(5)(float64),
				histogram(5)(int8N),
				histogram(5)(int16N),
				histogram(5)(int32N),
				histogram(5)(int64N),
				histogram(5)(uint8N),
				histogram(5)(uint16N),
				histogram(5)(uint32N),
				histogram(5)(uint64N),
				histogram(5)(float32N),
				histogram(5)(float64N)
 			FROM clickhouse_test_histogram
 		`
	)

	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if tx, err := connect.Begin(); assert.NoError(t, err) {
			if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_histogram"); assert.NoError(t, err) {
				if _, err := tx.Exec(ddl); assert.NoError(t, err) {
					if tx, err := connect.Begin(); assert.NoError(t, err) {
						if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
							for i := 0; i < 10; i++ {
								if _, err := stmt.Exec(
									8+i,
									16+i,
									32+i,
									64+i,
									18+i,
									116+i,
									132+i,
									165+i,
									1.1+float64(i),
									2.2+float64(i),
									new(int8),
									16+i,
									new(int32),
									64+i,
									18+i,
									116+i,
									nil,
									165+i,
									1.1+float64(i),
									2.2+float64(i),
								); !assert.NoError(t, err) {
									t.Fatal(err)
								}
							}
						}
						if err := tx.Commit(); !assert.NoError(t, err) {
							t.Fatal(err)
						}
					}
					if rows, err := connect.Query(query); assert.NoError(t, err) {
						for i := 0; rows.Next(); i++ {
							histos := make([][][]interface{}, 20)
							histoPtrs := make([]interface{}, 20)
							for i := range histos {
								histoPtrs[i] = &histos[i]
							}
							if err := rows.Scan(histoPtrs...); assert.NoError(t, err) {
								for _, histo := range histos {
									assert.IsType(t, [][]interface{}{}, histo)
									for _, bucket := range histo {
										assert.IsType(t, []interface{}{}, bucket)
										for _, f := range bucket {
											assert.IsType(t, float64(0), f)
										}
									}
								}
								t.Log(histos)
							}
						}
					}
				}
			}
		}
	}
}

func Test_ReadArrayArrayTuple(t *testing.T) {
	const (
		query = `
 			select
 			       [
 			           [(1.0, 2.0, 3.0)],
 			           [(4.0, 5.0, 6.0), (7.0, 8.0, 9.0)],
 			           [(10.0, 11.0, 12.0), (13.0, 14.0, 15.0), (16.0, 17.0, 18.0), (19.0, 20.0, 21.0), (22.0, 23.0, 24.0)]
				   ],
 			       number
			from numbers(2)
			group by number;
 		`
	)

	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if tx, err := connect.Begin(); assert.NoError(t, err) {
			if rows, err := tx.Query(query); assert.NoError(t, err) {
				for i := 0; rows.Next(); i++ {
					var (
						histArr [][][]interface{}
						group   string
					)
					if err := rows.Scan(&histArr, &group); assert.NoError(t, err) {
						assert.Len(t, histArr, 3)
						assert.Len(t, histArr[0], 1)
						assert.Len(t, histArr[0][0], 3)
						assert.Equal(t, []interface{}{1.0, 2.0, 3.0}, histArr[0][0])
						assert.Len(t, histArr[1], 2)
						assert.Len(t, histArr[1][0], 3)
						assert.Len(t, histArr[1][1], 3)
						assert.Equal(t, []interface{}{4.0, 5.0, 6.0}, histArr[1][0])
						assert.Equal(t, []interface{}{7.0, 8.0, 9.0}, histArr[1][1])
						assert.Len(t, histArr[2], 5)
						assert.Len(t, histArr[2][0], 3)
						assert.Len(t, histArr[2][1], 3)
						assert.Len(t, histArr[2][2], 3)
						assert.Len(t, histArr[2][3], 3)
						assert.Len(t, histArr[2][4], 3)
						assert.Equal(t, []interface{}{10.0, 11.0, 12.0}, histArr[2][0])
						assert.Equal(t, []interface{}{13.0, 14.0, 15.0}, histArr[2][1])
						assert.Equal(t, []interface{}{16.0, 17.0, 18.0}, histArr[2][2])
						assert.Equal(t, []interface{}{19.0, 20.0, 21.0}, histArr[2][3])
						assert.Equal(t, []interface{}{22.0, 23.0, 24.0}, histArr[2][4])
						for _, histo := range histArr {
							for _, tup := range histo {
								for _, f := range tup {
									assert.IsType(t, float64(0), f)
								}
							}
						}

						t.Log(histArr, group)
					}
				}
			}
		}
	}
}

func Test_RegisterDial(t *testing.T) {
	clickhouse.RegisterDial(func(network, address string, timeout time.Duration, config *tls.Config) (net.Conn, error) {
		return net.DialTimeout(network, address, timeout)
	})
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		assert.NoError(t, connect.Ping())
	}
	clickhouse.DeregisterDial()
}
