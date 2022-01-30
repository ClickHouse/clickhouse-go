package issues

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIssue164(t *testing.T) {
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		const ddl = `
		CREATE TEMPORARY TABLE issue_164 (
			  Col1 Int32
			, Col2 Array(Int8)
		)
		`
		if _, err := conn.Exec(ddl); assert.NoError(t, err) {
			scope, err := conn.Begin()
			if !assert.NoError(t, err) {
				return
			}
			if batch, err := scope.Prepare("INSERT INTO issue_164"); assert.NoError(t, err) {
				stmtParams := make([]interface{}, 0)
				stmtParams = append(stmtParams, sql.NamedArg{Name: "id", Value: int32(10)})
				stmtParams = append(stmtParams, sql.NamedArg{Name: "anything", Value: nil})
				if _, err := batch.ExecContext(context.Background(), stmtParams...); assert.Error(t, err) {
					assert.Contains(t, err.Error(), "converting <nil> to Array(Int8) is unsupported")
				}
			}
		}
	}
}
