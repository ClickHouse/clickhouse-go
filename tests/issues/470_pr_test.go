package issues

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test470PR(t *testing.T) {
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		const ddl = `
		CREATE TEMPORARY TABLE issue_470_pr (
			Col1 Array(String)
		)
		`
		if _, err := conn.Exec(ddl); assert.NoError(t, err) {
			scope, err := conn.Begin()
			if !assert.NoError(t, err) {
				return
			}
			if batch, err := scope.Prepare("INSERT INTO issue_470_pr"); assert.NoError(t, err) {
				if _, err := batch.Exec(nil); assert.Error(t, err) {
					assert.Contains(t, err.Error(), "converting <nil> to Array(String) is unsupported")
				}
			}
		}
	}
}
