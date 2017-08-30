package clickhouse_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Custom_Types(t *testing.T) {
	type (
		T_Int8        int8
		T_Int16       int16
		T_Int32       int32
		T_Int64       int64
		T_UInt8       uint8
		T_UInt16      uint16
		T_UInt32      uint32
		T_UInt64      uint64
		T_Float32     float32
		T_Float64     float64
		T_String      string
		T_FixedString string
	)
	const (
		ddl = `
			CREATE TABLE clickhouse_test_custom_types (
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
				fString FixedString(2)
			) Engine=Memory
		`
		dml = `
			INSERT INTO clickhouse_test_custom_types (
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
				fString
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
				fString
			FROM clickhouse_test_custom_types
		`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_custom_types"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						for i := 1; i <= 10; i++ {
							_, err = stmt.Exec(
								T_Int8(-1*i),
								T_Int16(-2*i),
								T_Int32(-4*i),
								T_Int64(-8*i), // int
								T_UInt8(1*i),
								T_UInt16(2*i),
								T_UInt32(4*i),
								T_UInt64(8*i), // uint
								T_Float32(1.32*float32(i)),
								T_Float64(1.64*float64(i)),            //float
								T_String(fmt.Sprintf("string %d", i)), // string
								T_FixedString("RU"),                   //fixedstring,

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
							Int8        T_Int8
							Int16       T_Int16
							Int32       T_Int32
							Int64       T_Int64
							UInt8       T_UInt8
							UInt16      T_UInt16
							UInt32      T_UInt32
							UInt64      T_UInt64
							Float32     T_Float32
							Float64     T_Float64
							String      T_String
							FixedString T_FixedString
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

type PointType struct {
	x int32
	y int32
	z int32
}

func (p PointType) Value() (driver.Value, error) {
	return fmt.Sprintf("%v,%v,%v", p.x, p.y, p.z), nil
}

func (p *PointType) Scan(v interface{}) error {
	var src string
	switch v := v.(type) {
	case string:
		src = v
	case []byte:
		src = string(v)
	default:
		return fmt.Errorf("unexpected type '%T'", v)
	}
	if _, err := fmt.Sscanf(src, "%d,%d,%d", &p.x, &p.y, &p.z); err != nil {
		return err
	}
	return nil
}

func Test_Scan_Value(t *testing.T) {
	const (
		ddl = `
	CREATE TABLE clickhouse_test_scan_value (
		Value String
	) Engine = Memory
	`
	)

	point := PointType{1, 2, 3}
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_scan_value"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					if stmt, err := tx.Prepare(`INSERT INTO clickhouse_test_scan_value VALUES (?)`); assert.NoError(t, err) {
						if _, err = stmt.Exec(point); !assert.NoError(t, err) {
							return
						}
					} else {
						return
					}
					if assert.NoError(t, tx.Commit()) {
						var p PointType
						if err := connect.QueryRow(`SELECT Value FROM clickhouse_test_scan_value`).Scan(&p); assert.NoError(t, err) {
							assert.Equal(t, point, p)
						}
					}
				}
			}
		}
	}
}
