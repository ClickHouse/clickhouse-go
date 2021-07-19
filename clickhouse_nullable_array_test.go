package clickhouse

import (
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"net"

	//"net"
	"testing"
	"time"
)

func Test_NullableArray(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_nullable_array
			(
				arr_int8     Array(Nullable(Int8)),
				arr_int16    Array(Nullable(Int16)),
				arr_int32    Array(Nullable(Int32)),
				arr_int64    Array(Nullable(Int64)),
				arr_uint8    Array(Nullable(UInt8)),
				arr_uint16   Array(Nullable(UInt16)),
				arr_uint32   Array(Nullable(UInt32)),
				arr_uint64   Array(Nullable(UInt64)),
				arr_float32  Array(Nullable(Float32)),
				arr_float64  Array(Nullable(Float64)),
				arr_ipv6     Array(Nullable(IPv6)),
				arr_ipv4     Array(Nullable(IPv4)),
				arr_string   Array(Nullable(String)),
				arr_date     Array(Nullable(Date)),
				arr_datetime Array(Nullable(DateTime))
			) Engine = Memory;
		`
		dml = `
			INSERT INTO clickhouse_test_nullable_array (
				arr_int8,
				arr_int16,
				arr_int32,
				arr_int64,

				arr_uint8,
				arr_uint16,
				arr_uint32,
				arr_uint64,
			                                            
				arr_float32,
				arr_float64,
			                                            
				arr_ipv6,
				arr_ipv4,
			                                            
				arr_string,
			                                            
				arr_date,
				arr_datetime
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
		query = `
			SELECT
				*
			FROM clickhouse_test_nullable_array
		`
	)

	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if tx, err := connect.Begin(); assert.NoError(t, err) {
			if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_nullable_array"); assert.NoError(t, err) {
				if _, err := tx.Exec(ddl); assert.NoError(t, err) {
					if tx, err := connect.Begin(); assert.NoError(t, err) {
						stmt, err := tx.Prepare(dml)
						if assert.NoError(t, err) {
							for i := 0; i < 100; i++ {
								int8V := int8(123)
								int16V := int16(i + 123)
								int32V := int32(i + i + 123)
								int64V := int64(123)

								uint8V := uint8(123)
								uint16V := uint16(i + 123)
								uint32V := uint32(i + i + 123)
								uint64V := uint64(123)

								float32V := float32(123.5)
								float64V := 12332.5

								ipv4V := net.ParseIP("1.1.1.1")
								ipv6V := net.ParseIP("2001:0db8:85a3:0000:0000:8a2e:0370:7334")

								stringV := "12343"
								timeV := time.Now()
								if _, err := stmt.Exec(
									[]*int8{&int8V, nil, &int8V},
									[]*int16{&int16V, nil, &int16V},
									[]*int32{&int32V, nil, &int32V},
									[]*int64{&int64V, nil, &int64V},

									[]*uint8{&uint8V, nil, &uint8V},
									[]*uint16{&uint16V, nil, &uint16V},
									[]*uint32{&uint32V, nil, &uint32V},
									[]*uint64{&uint64V, nil, &uint64V},

									[]*float32{&float32V, nil, &float32V},
									[]*float64{&float64V, nil, &float64V},

									[]*net.IP{&ipv4V, nil, &ipv4V},
									[]*net.IP{&ipv6V, nil, &ipv6V},

									[]*string{&stringV, nil, &stringV},

									[]*time.Time{&timeV, nil, &timeV},
									[]*time.Time{&timeV, nil, &timeV},

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
								ArrInt8     = make([]*int8, 0)
								ArrInt16    = make([]*int16, 0)
								ArrInt32    = make([]*int32, 0)
								ArrInt64    = make([]*int64, 0)
								ArrUInt8    = make([]*uint8, 0)
								ArrUInt16   = make([]*uint16, 0)
								ArrUInt32   = make([]*uint32, 0)
								ArrUInt64   = make([]*uint64, 0)
								ArrFloat32  = make([]*float32, 0)
								ArrFloat64  = make([]*float64, 0)
								ArrIpv6     = make([]*net.IP, 0)
								ArrIpv4     = make([]*net.IP, 0)
								ArrString   = make([]*string, 0)
								ArrDate     = make([]*time.Time, 0)
								ArrDateTime = make([]*time.Time, 0)
							)
							if err := rows.Scan(
								&ArrInt8,
								&ArrInt16,
								&ArrInt32,
								&ArrInt64,
								&ArrUInt8,
								&ArrUInt16,
								&ArrUInt32,
								&ArrUInt64,
								&ArrFloat32,
								&ArrFloat64,
								&ArrIpv6,
								&ArrIpv4,
								&ArrString,
								&ArrDate,
								&ArrDateTime,
							); assert.NoError(t, err) {
								fmt.Printf("ok")
							}
						}
					}
				}

			}
		}
	}
}
