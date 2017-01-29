package clickhouse_test

import (
	"database/sql"
	"testing"

	"github.com/kshvakov/clickhouse"
	"github.com/stretchr/testify/assert"
)

func Test_Negative_OpenConnectAndPing(t *testing.T) {
	if connect, err := sql.Open("clickhouse", ""); assert.NoError(t, err) {
		assert.Error(t, connect.Ping())
	}
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:10000"); assert.NoError(t, err) {
		assert.Error(t, connect.Ping())
	}
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?username=invalid"); assert.NoError(t, err) {
		if err := connect.Ping(); assert.Error(t, err) {
			if exception, ok := err.(*clickhouse.Exception); assert.True(t, ok) {
				assert.Equal(t, int32(192), exception.Code)
			}
		}
	}
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?password=invalid"); assert.NoError(t, err) {
		if err := connect.Ping(); assert.Error(t, err) {
			if exception, ok := err.(*clickhouse.Exception); assert.True(t, ok) {
				assert.Equal(t, int32(193), exception.Code)
			}
		}
	}
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?database=invalid"); assert.NoError(t, err) {
		if err := connect.Ping(); assert.Error(t, err) {
			if exception, ok := err.(*clickhouse.Exception); assert.True(t, ok) {
				assert.Equal(t, int32(81), exception.Code)
			}
		}
	}
}
