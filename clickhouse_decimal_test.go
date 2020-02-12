package clickhouse_test

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Decimal(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_nullable (
				decimal  Decimal(18,5),
				decimalNullable  Nullable(Decimal(15,3))
			) Engine=Memory;
		`
		dml = `
			INSERT INTO clickhouse_test_nullable (
				decimal,
				decimalNullable
			) VALUES (
				?,
				?
			)
		`
		query = `
			SELECT
				decimal,
				decimalNullable
			FROM clickhouse_test_nullable
		`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if tx, err := connect.Begin(); assert.NoError(t, err) {
			if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_nullable"); assert.NoError(t, err) {
				if _, err := tx.Exec(ddl); assert.NoError(t, err) {
					if tx, err := connect.Begin(); assert.NoError(t, err) {
						if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
							for i := 0; i < 10; i++ {
								if _, err := stmt.Exec(
									16.55,
									nil,
								); !assert.NoError(t, err) {
									t.Fatal(err)
								}
							}
						}
						if err := tx.Commit(); !assert.NoError(t, err) {
							t.Fatal(err)
						}
					}
					if rows, err := connect.Query(query); assert.NoError(t, err) {
						columnTypes, err := rows.ColumnTypes()
						assert.NoError(t, err)
						for i, column := range columnTypes {
							switch i {
							case 0:
								nullable, nullableOk := column.Nullable()
								assert.False(t, nullable)
								assert.True(t, nullableOk)

								precision, scale, ok := column.DecimalSize()
								assert.Equal(t, int64(5), scale)
								assert.Equal(t, int64(18), precision)
								assert.True(t, ok)
							case 1:
								nullable, nullableOk := column.Nullable()
								assert.True(t, nullable)
								assert.True(t, nullableOk)

								precision, scale, ok := column.DecimalSize()
								assert.Equal(t, int64(3), scale)
								assert.Equal(t, int64(15), precision)
								assert.True(t, ok)
							}
						}
						for rows.Next() {
							var (
								decimal         = new(int)
								decimalNullable = new(int)
							)
							if err := rows.Scan(
								&decimal,
								&decimalNullable,
							); assert.NoError(t, err) {
								if assert.NotNil(t, decimal) {
									assert.Equal(t, int(1655000), *decimal)
								}
								assert.Nil(t, decimalNullable)
							}
						}
					}
				}
			}
		}
	}
}
