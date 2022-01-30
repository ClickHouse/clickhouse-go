package issues

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIssue357(t *testing.T) {
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		const ddl = ` -- foo.bar DDL comment
		CREATE TEMPORARY TABLE issue_357 (
			  Col1 Int32
			, Col2 DateTime
		)
		`
		if _, err := conn.Exec(ddl); assert.NoError(t, err) {
			scope, err := conn.Begin()
			if !assert.NoError(t, err) {
				return
			}
			const query = ` -- foo.bar Insert comment
				INSERT INTO issue_357
				`
			if batch, err := scope.Prepare(query); assert.NoError(t, err) {
				if _, err := batch.Exec(int32(42), time.Now()); assert.NoError(t, err) {
					if err := scope.Commit(); assert.NoError(t, err) {
						var (
							col1 int32
							col2 time.Time
						)
						if err := conn.QueryRow("SELECT * FROM issue_357").Scan(&col1, &col2); assert.NoError(t, err) {
							assert.Equal(t, int32(42), col1)
						}
					}
				}
			}
		}
	}
}
