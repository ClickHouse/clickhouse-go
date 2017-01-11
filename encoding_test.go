package clickhouse

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Encode(t *testing.T) {
	var (
		date, _     = time.Parse("2006-01-02", "2017-01-11")
		datetime, _ = time.Parse("2006-01-02 15:04:05", "2017-01-11 13:08:59")
		assets      = map[driver.Value]string{
			"string":           "'string'",
			`ecaped ' \string`: `'ecaped \' \\string'`,
			42:                 "42",
			42.1:               "42.1",
			date:               "'2017-01-11'",
			datetime:           "'2017-01-11 13:08:59'",
		}
	)

	for value, expected := range assets {
		assert.Equal(t, expected, encode(value))
	}
}

func Test_Decode(t *testing.T) {
	if v, err := decode("String", []byte(`ecaped \' \\string`)); assert.NoError(t, err) {
		if value, ok := v.([]byte); assert.True(t, ok) {
			assert.Equal(t, `ecaped ' \string`, string(value))
		}
	}

	if v, err := decode("Date", []byte(`2017-01-11`)); assert.NoError(t, err) {
		if value, ok := v.(time.Time); assert.True(t, ok) {
			if expected, err := time.Parse("2006-01-02", "2017-01-11"); assert.NoError(t, err) {
				assert.Equal(t, expected, value)
			}
		}
	}

	if v, err := decode("DateTime", []byte(`2017-01-11 13:08:59`)); assert.NoError(t, err) {
		if value, ok := v.(time.Time); assert.True(t, ok) {
			if expected, err := time.Parse("2006-01-02 15:04:05", "2017-01-11 13:08:59"); assert.NoError(t, err) {
				assert.Equal(t, expected, value)
			}
		}
	}
}

func Benchmark_Encode(b *testing.B) {
	v := time.Now()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encode(v)
	}
}

func Benchmark_Decode(b *testing.B) {
	v := time.Now()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		decode("String", []byte(v.String()+"\\'"))
	}
}
