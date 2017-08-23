package clickhouse_test

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/kshvakov/clickhouse"
	"github.com/stretchr/testify/assert"
)

func Test_ColumnarInsert(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_columnar_insert (
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
				enum16   Enum16('c' = 1, 'd' = 2)
			) Engine=Memory
		`
		dml = `
			INSERT INTO clickhouse_test_columnar_insert (
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
				?
			)
		`
	)
	if connect, err := clickhouse.OpenDirect("tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		{
			connect.Begin()
			stmt, _ := connect.Prepare("DROP TABLE clickhouse_test_columnar_insert")
			stmt.Exec([]driver.Value{})
			connect.Commit()
		}
		{
			if _, err := connect.Begin(); assert.NoError(t, err) {
				if stmt, err := connect.Prepare(ddl); assert.NoError(t, err) {
					if _, err := stmt.Exec([]driver.Value{}); assert.NoError(t, err) {
						assert.NoError(t, connect.Commit())
					}
				}
			}
		}
		{
			if _, err := connect.Begin(); assert.NoError(t, err) {
				if _, err := connect.Prepare(dml); assert.NoError(t, err) {
					block, err := connect.Block()
					assert.NoError(t, err)
					block.Reserve()
					block.NumRows = 100

					for i := 0; i < 100; i++ {
						block.WriteUInt8(0, uint8(i))
						block.WriteUInt16(1, uint16(i))
						block.WriteUInt32(2, uint32(i))
						block.WriteUInt64(3, uint64(i))

						block.WriteFloat32(4, float32(i))
						block.WriteFloat64(5, float64(i))

						block.WriteString(6, "string")
						block.WriteFixedString(7, []byte("CH"))
						block.WriteDate(8, time.Now())
						block.WriteDateTime(9, time.Now())

						block.WriteUInt8(10, 1)
						block.WriteUInt16(11, 2)

						if !assert.NoError(t, err) {
							return
						}
					}

					assert.NoError(t, connect.Commit())
				}
			}
		}
	}
}
