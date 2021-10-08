package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_ConnCheck(t *testing.T) {
	const (
		ddl = `
				CREATE TABLE clickhouse_test_conncheck (
						Value String
				) Engine = Memory
		`
		dml = `
				INSERT INTO clickhouse_test_conncheck
				VALUES (?)
		`
	)

	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=false"); assert.NoError(t, err) {
		// We can only change the settings at the connection level.
		// If we have only one connection, we change the settings specifically for that connection.
		connect.SetMaxOpenConns(1)
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_conncheck"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				_, err = connect.Exec("set idle_connection_timeout=1")
				assert.NoError(t, err)

				_, err = connect.Exec("set tcp_keep_alive_timeout=0")
				assert.NoError(t, err)

				time.Sleep(1100 * time.Millisecond)
				ctx := context.Background()
				tx, err := connect.BeginTx(ctx, nil)
				assert.NoError(t, err)

				_, err = tx.PrepareContext(ctx, dml)
				assert.NoError(t, err)
			}
		}
	}
}

func Test_ConnCheckNegative(t *testing.T) {
	const (
		ddl = `
				CREATE TABLE clickhouse_test_conncheck_negative (
						Value String
				) Engine = Memory
		`
		dml = `
				INSERT INTO clickhouse_test_conncheck_negative
				VALUES (?)
		`
	)

	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true&check_connection_liveness=false"); assert.NoError(t, err) {
		// We can only change the settings at the connection level.
		// If we have only one connection, we change the settings specifically for that connection.
		connect.SetMaxOpenConns(1)
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_conncheck_negative"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				_, err = connect.Exec("set idle_connection_timeout=1")
				assert.NoError(t, err)

				_, err = connect.Exec("set tcp_keep_alive_timeout=0")
				assert.NoError(t, err)

				time.Sleep(1100 * time.Millisecond)
				ctx := context.Background()
				tx, err := connect.BeginTx(ctx, nil)
				assert.NoError(t, err)

				_, err = tx.PrepareContext(ctx, dml)
				assert.Equal(t, driver.ErrBadConn, err)
			}
		}
	}
}
