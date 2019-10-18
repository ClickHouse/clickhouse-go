package clickhouse_test

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

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
