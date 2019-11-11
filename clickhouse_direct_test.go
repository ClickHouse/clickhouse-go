package clickhouse_test

import (
	"database/sql/driver"
	"fmt"

	//	"fmt"
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/ClickHouse/clickhouse-go/lib/column"
	"github.com/ClickHouse/clickhouse-go/lib/types"
	"github.com/stretchr/testify/assert"
)

func Test_DirectInsert(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_direct_insert (
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
				enum8    Enum8 ('a' = 1, 'b' = 2),
				enum16   Enum16('c' = 1, 'd' = 2),
				uuid     FixedString(16),
				ip       FixedString(16)
			) Engine=Memory
		`
		dml = `
			INSERT INTO clickhouse_test_direct_insert (
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
				uuid,
				ip
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
	)
	if connect, err := clickhouse.Open("tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		{
			var (
				tx, _   = connect.Begin()
				stmt, _ = connect.Prepare("DROP TABLE IF EXISTS clickhouse_test_direct_insert")
			)
			stmt.Exec([]driver.Value{})
			tx.Commit()
		}
		{
			if tx, err := connect.Begin(); assert.NoError(t, err) {
				if stmt, err := connect.Prepare(ddl); assert.NoError(t, err) {
					if _, err := stmt.Exec([]driver.Value{}); assert.NoError(t, err) {
						assert.NoError(t, tx.Commit())
					}
				}
			}
		}
		{
			if tx, err := connect.Begin(); assert.NoError(t, err) {
				if stmt, err := connect.Prepare(dml); assert.NoError(t, err) {
					for i := 0; i < 100; i++ {
						_, err := stmt.Exec([]driver.Value{
							int8(i),
							int16(i),
							int32(i),
							int64(i),

							uint8(i),
							uint16(i),
							uint32(i),
							uint64(i),

							float32(i),
							float64(i),

							"string",
							"CH",
							time.Now(),
							time.Now(),

							"a",
							"d",

							types.UUID("123e4567-e89b-12d3-a456-426655440000"),
							column.IP(net.ParseIP("127.0.0.1")),
						})
						if !assert.NoError(t, err) {
							return
						}
					}
					assert.NoError(t, tx.Commit())
				}
			}
		}
	}
}

func Test_DirectArrayT(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_direct_array (
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
				ipv4     Array(IPv4),
				ipv6     Array(IPv6)
			) Engine=Memory
		`
		dml = `
			INSERT INTO clickhouse_test_direct_array (
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
				?,
				?,
				?,
				?,
				?,
				?,
				?
			)
		`
	)

	if connect, err := clickhouse.Open("tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		{
			var (
				tx, _   = connect.Begin()
				stmt, _ = connect.Prepare("DROP TABLE IF EXISTS clickhouse_test_direct_array")
			)
			stmt.Exec([]driver.Value{})
			tx.Commit()
		}
		{
			if tx, err := connect.Begin(); assert.NoError(t, err) {
				if stmt, err := connect.Prepare(ddl); assert.NoError(t, err) {
					if _, err := stmt.Exec([]driver.Value{}); assert.NoError(t, err) {
						assert.NoError(t, tx.Commit())
					}
				}
			}
		}
		{
			if tx, err := connect.Begin(); assert.NoError(t, err) {
				if stmt, err := connect.Prepare(dml); assert.NoError(t, err) {
					for i := 0; i < 100; i++ {
						_, err := stmt.Exec([]driver.Value{
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
							clickhouse.Array([]string{"a", "b"}),
							clickhouse.Array([]string{"c", "d"}),
							clickhouse.Array([]string{"1.2.3.4", "2.2.3.4"}),
							clickhouse.Array([]string{"2001:0db8:85a3:0000:0000:8a2e:0370:7334"}),
						})
						if !assert.NoError(t, err) {
							return
						}
					}
					assert.NoError(t, tx.Commit())
				}
			}
		}
	}
}
