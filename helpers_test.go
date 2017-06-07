package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ToColumnType(t *testing.T) {
	assets := map[string]interface{}{
		"Date":           Date{},
		"DateTime":       DateTime{},
		"String":         string(""),
		"Int8":           int8(0),
		"Int16":          int16(0),
		"Int32":          int32(0),
		"Int64":          int64(0),
		"UInt8":          uint8(0),
		"UInt16":         uint16(0),
		"UInt32":         uint32(0),
		"UInt64":         uint64(0),
		"Float32":        float32(0),
		"Float64":        float64(0),
		"FixedString(2)": []byte{0, 0},
		"Enum8('a' = 2)": enum8(enum{
			baseType: int8(0),
			iv: map[string]interface{}{
				"a": int8(2),
			},
			vi: map[interface{}]string{
				int8(2): "a",
			},
		}),
		"Enum16('a' = 2)": enum16(enum{
			baseType: int16(0),
			iv: map[string]interface{}{
				"a": int16(2),
			},
			vi: map[interface{}]string{
				int16(2): "a",
			},
		}),
		"Array(Int8)": array{baseType: int8(0)},
	}
	for ct, expected := range assets {
		if actual, err := toColumnType(ct); assert.NoError(t, err) {
			assert.Equal(t, expected, actual)
		}
	}
	if _, err := toColumnType("Unhandled column type"); assert.Error(t, err) {
		for _, invalidColumn := range []string{
			"FixedString",
			"FixedString(N)",
			"Enum8",
			"Enum8(ident)",
			"Enum16",
			"Enum16(ident)",
			"Array",
			"Array(N)",
		} {
			if _, err := toColumnType(invalidColumn); !assert.Error(t, err) {
				return
			}
		}
	}
}

func Test_NumInput(t *testing.T) {
	for query, num := range map[string]int{
		"SELECT * FROM example WHERE os_id = 42":                                                  0,
		"SELECT * FROM example WHERE email = 'name@mail'":                                         0,
		"SELECT * FROM example WHERE os_id = @os_id AND browser_id = @os_id":                      1,
		"SELECT * FROM example WHERE os_id = @os_id AND browser_id = @os_id2":                     2,
		"SELECT * FROM example WHERE os_id in (@os_id,@browser_id) browser_id = @browser_id":      2,
		"SELECT * FROM example WHERE os_id IN (@os_id, @browser_id) AND browser_id = @browser_id": 2,
		"SELECT * FROM example WHERE os_id = ? AND browser_id = ?":                                2,
		"SELECT * FROM example WHERE os_id in (?,?) browser_id = ?":                               3,
		"SELECT * FROM example WHERE os_id IN (?, ?) AND browser_id = ?":                          3,
		"SELECT a ? '+' : '-'":                                                                    0,
		"SELECT a ? '+' : '-' FROM example WHERE a = ? AND b IN(?)":                               2,
		`SELECT 
			a ? '+' : '-' 
		FROM example WHERE a = 42 and b in(
			?,
			?,
			?
		)
		`: 3,
	} {
		assert.Equal(t, num, numInput(query), query)
	}
}

func Benchmark_NumInput(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		numInput("SELECT * FROM example WHERE os_id in (@os_id,@browser_id) browser_id = @browser_id")
	}
}
