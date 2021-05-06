package clickhouse_test

import (
	"database/sql/driver"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/ClickHouse/clickhouse-go/lib/data"
	"github.com/stretchr/testify/assert"
	"net"
	"reflect"
	"testing"
	"time"
)

func Test_ColumnarInsert(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_columnar_insert (
				uint8  UInt8,
				uint8_null Nullable(UInt8),
				uint16 UInt16,
				uint16_null Nullable(UInt16),
				uint32 UInt32,
				uint32_null Nullable(UInt32),
				uint64 UInt64,
				uint64_null Nullable(UInt64),
				float32 Float32,
				float32_null Nullable(Float32),
				float64 Float64,
				float64_null Nullable(Float64),
				string  String,
				string_null  Nullable(String),
				fString FixedString(2),
				fString_null Nullable(FixedString(2)),
				date    Date,
				date_null    Nullable(Date),
				datetime   DateTime,
				datetime_null   Nullable(DateTime),
				enum8      Enum8 ('a' = 1, 'b' = 2),
				enum8_null      Nullable(Enum8 ('a' = 1, 'b' = 2)),
				enum16     Enum16('c' = 1, 'd' = 2),
				enum16_null     Nullable(Enum16('c' = 1, 'd' = 2)),
				array      Array(String),
				arrayArray Array(Array(String)),
				arrayWithValue Array(UInt64),
				arrayWithValueFast Array(UInt64),
				ipv4 IPv4,
				ipv6 IPv6
			) Engine=Memory
		`
		dml = `
			INSERT INTO clickhouse_test_columnar_insert (
				uint8,
				uint8_null,
				uint16,
				uint16_null,
				uint32,
				uint32_null,
				uint64,
				uint64_null,
				float32,
				float32_null,
				float64,
				float64_null,
				string,
				string_null,
				fString,
				fString_null,
	 			date,
				date_null,
				datetime,
				datetime_null,
				enum8,
				enum8_null,
				enum16,
				enum16_null,
				array,
				arrayArray,
				arrayWithValue,
				arrayWithValueFast,
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
					?,
					?,
					?
			)`
	)
	if connect, err := clickhouse.OpenDirect("tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		{
			connect.Begin()
			stmt, _ := connect.Prepare("DROP TABLE IF EXISTS clickhouse_test_columnar_insert")
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
						block.WriteUInt8Nullable(1, nil)
						block.WriteUInt16(2, uint16(i))
						block.WriteUInt16Nullable(3, nil)
						block.WriteUInt32(4, uint32(i))
						block.WriteUInt32Nullable(5, nil)
						block.WriteUInt64(6, uint64(i))
						block.WriteUInt64Nullable(7, nil)

						block.WriteFloat32(8, float32(i))
						block.WriteFloat32Nullable(9, nil)
						block.WriteFloat64(10, float64(i))
						block.WriteFloat64Nullable(11, nil)

						block.WriteString(12, "string")
						block.WriteStringNullable(13, nil)
						block.WriteFixedString(14, []byte("CH"))
						block.WriteFixedStringNullable(15, nil)
						block.WriteDate(16, time.Now())
						block.WriteDateNullable(17, nil)
						block.WriteDateTime(18, time.Now())
						block.WriteDateTimeNullable(19, nil)

						block.WriteUInt8(20, 1)
						block.WriteUInt8Nullable(21, nil)
						block.WriteUInt16(22, 2)
						block.WriteUInt16Nullable(23, nil)
						block.WriteArray(24, []string{"A", "B", "C"})

						block.WriteArray(25, [][]string{[]string{"A", "B"}, []string{"CC", "DD", "EE"}})
						block.WriteArrayWithValue(26, newUint64SliceValueSimple([]uint64{1, 2, 3}))
						block.WriteArrayWithValue(27, newUint64SliceValueFast([]uint64{10, 20, 30}))
						block.WriteIP(28, net.ParseIP("213.180.204.62"))
						block.WriteIP(29, net.ParseIP("2606:4700:5c::a29f:2e07"))
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

type uint64Value struct {
	value uint64
}

func (v *uint64Value) Kind() reflect.Kind {
	return reflect.String
}

func (v *uint64Value) Len() int {
	panic("uint64 has no length")
}

func (v *uint64Value) Index(i int) data.Value {
	panic("uint64 has no index")
}

func (v *uint64Value) Interface() interface{} {
	return v.value
}

type uint64SliceValueSimple struct {
	uint64Slice []uint64
}

func newUint64SliceValueSimple(v []uint64) *uint64SliceValueSimple {
	return &uint64SliceValueSimple{uint64Slice: v}
}

func (v *uint64SliceValueSimple) Kind() reflect.Kind {
	return reflect.Slice
}

func (v *uint64SliceValueSimple) Len() int {
	return len(v.uint64Slice)
}

func (v *uint64SliceValueSimple) Index(i int) data.Value {
	return &uint64Value{value: v.uint64Slice[i]}
}

func (v *uint64SliceValueSimple) Interface() interface{} {
	return v.uint64Slice
}

type uint64SliceValueFast struct {
	uint64Slice []uint64
	uint64Value *uint64Value
	value       data.Value
}

func newUint64SliceValueFast(v []uint64) *uint64SliceValueFast {
	var uint64Value uint64Value
	return &uint64SliceValueFast{
		uint64Slice: v,
		uint64Value: &uint64Value,
		value:       &uint64Value,
	}
}

func (v *uint64SliceValueFast) Kind() reflect.Kind {
	return reflect.Slice
}

func (v *uint64SliceValueFast) Len() int {
	return len(v.uint64Slice)
}

func (v *uint64SliceValueFast) Index(i int) data.Value {
	v.uint64Value.value = v.uint64Slice[i]
	// NB: This avoids the CPU cost of converting *uint64Value to data.Value.
	return v.value
}

func (v *uint64SliceValueFast) Interface() interface{} {
	return v.uint64Slice
}
