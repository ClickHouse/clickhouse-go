package std

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBool(t *testing.T) {
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 21, 12); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE TABLE test_bool (
				    Col1 Bool
				  , Col2 Bool
				  , Col3 Nullable(Bool)
			) Engine Memory
		`
		if _, err := conn.Exec("DROP TABLE IF EXISTS test_bool"); assert.NoError(t, err) {
			if _, err := conn.Exec(ddl); assert.NoError(t, err) {
				scope, err := conn.Begin()
				if !assert.NoError(t, err) {
					return
				}
				if batch, err := scope.Prepare("INSERT INTO test_bool"); assert.NoError(t, err) {
					if _, err := batch.Exec(true, false, true); assert.NoError(t, err) {
						if err := scope.Commit(); assert.NoError(t, err) {
							var (
								col1 bool
								col2 bool
								col3 *bool
							)
							if err := conn.QueryRow("SELECT * FROM test_bool").Scan(&col1, &col2, &col3); assert.NoError(t, err) {
								assert.Equal(t, true, col1)
								assert.Equal(t, false, col2)
								assert.Equal(t, true, *col3)
							}
						}
					}
				}
			}
		}
	}
}
