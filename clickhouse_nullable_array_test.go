package clickhouse

import (
	"database/sql"
	"net"

	"github.com/stretchr/testify/assert"

	"testing"
	"time"
)

func Test_NullableArray(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_nullable_array
			(
				arr_decimal    Array(Nullable(Decimal(15, 3))),
				arr_int8       Array(Nullable(Int8)),
				arr_int16      Array(Nullable(Int16)),
				arr_int32      Array(Nullable(Int32)),
				arr_int64      Array(Nullable(Int64)),
				arr_uint8      Array(Nullable(UInt8)),
				arr_uint16     Array(Nullable(UInt16)),
				arr_uint32     Array(Nullable(UInt32)),
				arr_uint64     Array(Nullable(UInt64)),
				arr_float32    Array(Nullable(Float32)),
				arr_float64    Array(Nullable(Float64)),
				arr_ipv6       Array(Nullable(IPv6)),
				arr_ipv4       Array(Nullable(IPv4)),
				arr_string     Array(Nullable(String)),
				arr_arr_string Array(Array(Nullable(String))),
				arr_date       Array(Nullable(Date)),
				arr_datetime   Array(Nullable(DateTime)),
				arr_enum8_str  Array(Nullable(Enum8('a8' = 1, 'b8' = 2))),
				arr_enum8_int  Array(Nullable(Enum8('a8' = 1, 'b8' = 2))),
				arr_enum16_str Array(Nullable(Enum16('a16' = 1, 'b16' = 2))),
				arr_enum16_int Array(Nullable(Enum16('a16' = 1, 'b16' = 2)))
			) Engine = Memory;
		`
		dml = `
			INSERT INTO clickhouse_test_nullable_array (arr_decimal,
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
														arr_arr_string,
														arr_date,
														arr_datetime,
														arr_enum8_str,
														arr_enum8_int,
														arr_enum16_str,
														arr_enum16_int)
			VALUES (?,
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
					?);
		`
		query = `
			SELECT
				*
			FROM clickhouse_test_nullable_array
		`
	)

	decV := 16.55
	int64Dec := int64(16550)
	int8V := int8(123)
	int16V := int16(1231)
	int32V := int32(12312)
	int64V := int64(123123)

	uint8V := uint8(123)
	uint16V := uint16(1231)
	uint32V := uint32(12312)
	uint64V := uint64(123123)

	float32V := float32(123.123)
	float64V := 123123.123123

	stringV := "123123"

	ipv6V := net.ParseIP("2001:0db8:85a3:0000:0000:8a2e:0370:7334")
	ipv4V := net.ParseIP("123.123.123.123")

	timeV, _ := time.Parse("2006-01-02 15:04:05", "2021-07-11 00:00:00")
	dateV, _ := time.Parse("2006-01-02", "2021-07-11")

	enum8VA := "a8"
	enum8VB := "b8"

	enum8V1 := int8(1)
	enum8V2 := int8(2)

	enum16VA := "a16"
	enum16VB := "b16"

	enum16V1 := int16(1)
	enum16V2 := int16(2)

	var timeNil *time.Time

	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if tx, err := connect.Begin(); assert.NoError(t, err) {
			if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_nullable_array"); assert.NoError(t, err) {
				if _, err := tx.Exec(ddl); assert.NoError(t, err) {
					if tx, err := connect.Begin(); assert.NoError(t, err) {
						stmt, err := tx.Prepare(dml)
						if assert.NoError(t, err) {
							for i := 0; i < 10; i++ {
								if _, err := stmt.Exec(
									[]*float64{&decV, nil, &decV},
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

									[]*net.IP{&ipv6V, nil, &ipv6V},
									[]*net.IP{&ipv4V, nil, &ipv4V},

									[]*string{&stringV, nil, &stringV},
									[][]*string{{&stringV, nil, &stringV}},

									[]*time.Time{&dateV, nil, &dateV},
									[]*time.Time{&timeV, nil, &timeV},

									[]*string{&enum8VA, nil, &enum8VB},
									[]*int8{&enum8V1, nil, &enum8V2},
									[]*string{&enum16VA, nil, &enum16VB},
									[]*int16{&enum16V1, nil, &enum16V2},
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
						for i := 0; i < 10; i++ {
							rows.Next()
							var (
								ArrDecimal     = make([]*int64, 0)
								ArrInt8        = make([]*int8, 0)
								ArrInt16       = make([]*int16, 0)
								ArrInt32       = make([]*int32, 0)
								ArrInt64       = make([]*int64, 0)
								ArrUInt8       = make([]*uint8, 0)
								ArrUInt16      = make([]*uint16, 0)
								ArrUInt32      = make([]*uint32, 0)
								ArrUInt64      = make([]*uint64, 0)
								ArrFloat32     = make([]*float32, 0)
								ArrFloat64     = make([]*float64, 0)
								ArrIpv6        = make([]*net.IP, 0)
								ArrIpv4        = make([]*net.IP, 0)
								ArrString      = make([]*string, 0)
								ArrArrString   = make([][]*string, 0)
								ArrDate        = make([]*time.Time, 0)
								ArrDateTime    = make([]*time.Time, 0)
								ArrEnum8Str    = make([]*string, 0)
								ArrEnum8Int8   = make([]*string, 0)
								ArrEnum16Str   = make([]*string, 0)
								ArrEnum16Int16 = make([]*string, 0)
							)
							if err := rows.Scan(
								&ArrDecimal,
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
								&ArrArrString,
								&ArrDate,
								&ArrDateTime,
								&ArrEnum8Str,
								&ArrEnum8Int8,
								&ArrEnum16Str,
								&ArrEnum16Int16,
							); assert.NoError(t, err) {
								assert.Equal(t, ArrDecimal, []*int64{&int64Dec, nil, &int64Dec})
								assert.Equal(t, ArrInt8, []*int8{&int8V, nil, &int8V})
								assert.Equal(t, ArrInt16, []*int16{&int16V, nil, &int16V})
								assert.Equal(t, ArrInt32, []*int32{&int32V, nil, &int32V})
								assert.Equal(t, ArrInt64, []*int64{&int64V, nil, &int64V})

								assert.Equal(t, ArrUInt8, []*uint8{&uint8V, nil, &uint8V})
								assert.Equal(t, ArrUInt16, []*uint16{&uint16V, nil, &uint16V})
								assert.Equal(t, ArrUInt32, []*uint32{&uint32V, nil, &uint32V})
								assert.Equal(t, ArrUInt64, []*uint64{&uint64V, nil, &uint64V})

								assert.Equal(t, ArrFloat32, []*float32{&float32V, nil, &float32V})
								assert.Equal(t, ArrFloat64, []*float64{&float64V, nil, &float64V})

								assert.Equal(t, ArrIpv6, []*net.IP{&ipv6V, nil, &ipv6V})
								assert.Equal(t, ArrIpv4, []*net.IP{&ipv4V, nil, &ipv4V})

								assert.Equal(t, ArrString, []*string{&stringV, nil, &stringV})
								assert.Equal(t, ArrArrString, [][]*string{{&stringV, nil, &stringV}})

								assert.True(t, len(ArrDate) == 3)
								assert.True(t, len(ArrDateTime) == 3)
								assert.Equal(t, ArrDate[1], timeNil)
								assert.Equal(t, ArrDateTime[1], timeNil)

								assert.Equal(t, ArrEnum8Int8, []*string{&enum8VA, nil, &enum8VB})
								assert.Equal(t, ArrEnum16Int16, []*string{&enum16VA, nil, &enum16VB})
								assert.Equal(t, ArrEnum8Str, []*string{&enum8VA, nil, &enum8VB})
								assert.Equal(t, ArrEnum16Str, []*string{&enum16VA, nil, &enum16VB})
							} else {
								t.Fatal(err)
							}
						}
					} else {
						t.Fatal(err)
					}
				}
			}
		}
	}
}
