package std

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithTotals(t *testing.T) {
	const query = `
	SELECT
		number AS n
		, COUNT()
	FROM (
		SELECT number FROM system.numbers LIMIT 100
	) GROUP BY n WITH TOTALS
	`
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		if rows, err := conn.Query(query); assert.NoError(t, err) {
			var count int
			for rows.Next() {
				count++
				var (
					n uint64
					c uint64
				)
				if !assert.NoError(t, rows.Scan(&n, &c)) {
					return
				}
			}
			if assert.Equal(t, 100, count) {
				if assert.True(t, rows.NextResultSet()) {
					var count int
					for rows.Next() {
						count++
						var (
							n, totals uint64
						)
						if assert.NoError(t, rows.Scan(&n, &totals)) {
							assert.Equal(t, uint64(0), n)
							assert.Equal(t, uint64(100), totals)
						}
					}
					assert.Equal(t, 1, count)
				}
			}
		}
	}
}
