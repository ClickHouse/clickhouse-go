package clickhouse_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Nullable(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_nullable (
				int8     Nullable(Int8),
				int16    Nullable(Int16),
				int32    Nullable(Int32),
				int64    Nullable(Int64),
				uint8    Nullable(UInt8),
				uint16   Nullable(UInt16),
				uint32   Nullable(UInt32),
				uint64   Nullable(UInt64),
				float32  Nullable(Float32),
				float64  Nullable(Float64),
				string   Nullable(String),
				fString  Nullable(FixedString(2)),
				date     Nullable(Date),
				datetime Nullable(DateTime),
				enum8    Nullable(Enum8 ('a' = 1, 'b' = 2)),
				enum16   Nullable(Enum16('c' = 1, 'd' = 2))
			) Engine=Memory;
		`
		dml = `
			INSERT INTO clickhouse_test_nullable (
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
				datetime,
				enum8,
				enum16
			FROM clickhouse_test_nullable
		`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if tx, err := connect.Begin(); assert.NoError(t, err) {
			if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_nullable"); assert.NoError(t, err) {
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
									"UA",
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
						for rows.Next() {
							var (
								Int8     = new(int8)
								Int16    = new(int16)
								Int32    = new(int32)
								Int64    = new(int64)
								UInt8    = new(uint8)
								UInt16   = new(uint16)
								UInt32   = new(uint32)
								UInt64   = new(uint64)
								Float32  = new(float32)
								Float64  = new(float64)
								String   = new(string)
								FString  = new(string)
								Date     = new(time.Time)
								DateTime = new(time.Time)
								Enum8    = new(string)
								Enum16   = new(string)
							)
							if err := rows.Scan(
								&Int8,
								&Int16,
								&Int32,
								&Int64,
								&UInt8,
								&UInt16,
								&UInt32,
								&UInt64,
								&Float32,
								&Float64,
								&String,
								&FString,
								&Date,
								&DateTime,
								&Enum8,
								&Enum16,
							); assert.NoError(t, err) {
								if assert.NotNil(t, Int8) {
									assert.Equal(t, int8(8), *Int8)
								}
								if assert.NotNil(t, Int16) {
									assert.Equal(t, int16(16), *Int16)
								}
								if assert.NotNil(t, Int32) {
									assert.Equal(t, int32(32), *Int32)
								}
								if assert.NotNil(t, Int64) {
									assert.Equal(t, int64(64), *Int64)
								}

								if assert.NotNil(t, String) {
									assert.Equal(t, "RU", *String)
								}
								if assert.NotNil(t, FString) {
									assert.Equal(t, "UA", *FString)
								}
								t.Log(
									*Int8,
									*Int16,
									*Int32,
									*Int64,
									*UInt8,
									*UInt16,
									*UInt32,
									*UInt64,
									*Float32,
									*Float64,
									*String,
									*FString,
									*Date,
									*DateTime,
									*Enum8,
									*Enum16,
								)
							}
						}
					}
				}
			}
		}
	}

	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if tx, err := connect.Begin(); assert.NoError(t, err) {
			if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_nullable"); assert.NoError(t, err) {
				if _, err := tx.Exec(ddl); assert.NoError(t, err) {
					if tx, err := connect.Begin(); assert.NoError(t, err) {
						if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
							if _, err := stmt.Exec(
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
								"UA",
								time.Now(),
								time.Now(),
								"a",
								"c",
							); !assert.NoError(t, err) {
								t.Fatal(err)
							}
						}
						if err := tx.Commit(); !assert.NoError(t, err) {
							t.Fatal(err)
						}
					}
					if rows, err := connect.Query(query); assert.NoError(t, err) {
						if assert.True(t, rows.Next()) {
							var (
								Int8     = new(int8)
								Int16    = new(int16)
								Int32    = new(int32)
								Int64    = new(int64)
								UInt8    = new(uint8)
								UInt16   = new(uint16)
								UInt32   = new(uint32)
								UInt64   = new(uint64)
								Float32  = new(float32)
								Float64  = new(float64)
								String   = new(string)
								FString  = new(string)
								Date     = new(time.Time)
								DateTime = new(time.Time)
								Enum8    = new(string)
								Enum16   = new(string)
							)
							if err := rows.Scan(
								&Int8,
								&Int16,
								&Int32,
								&Int64,
								&UInt8,
								&UInt16,
								&UInt32,
								&UInt64,
								&Float32,
								&Float64,
								&String,
								&FString,
								&Date,
								&DateTime,
								&Enum8,
								&Enum16,
							); assert.NoError(t, err) {
								if assert.NotNil(t, Int8) {
									if assert.Equal(t, int8(0), *Int8) && assert.NotNil(t, Int16) {
										assert.Equal(t, int16(16), *Int16)
									}
								}
								if assert.NotNil(t, Int32) {
									if assert.Equal(t, int32(0), *Int32) && assert.NotNil(t, Int64) {
										assert.Equal(t, int64(64), *Int64)
									}
								}
								if assert.Nil(t, String) {
									if assert.NotNil(t, FString) {
										assert.Equal(t, "UA", *FString)
									}
								}
								t.Log(
									Int8,
									*Int16,
									Int32,
									*Int64,
									*UInt8,
									*UInt16,
									*UInt32,
									*UInt64,
									*Float32,
									*Float64,
									String,
									*FString,
									*Date,
									*DateTime,
									*Enum8,
									*Enum16,
								)
							}
						}
					}
				}
			}
		}
	}
}
