// +build go1.8

package clickhouse

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Issue38_uint64_support(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_uint64_support (
				A UInt64,
				B UInt64,
				C UInt64
			) Engine=Memory
		`
		dml = `
			INSERT INTO clickhouse_test_uint64_support (
				A,
				B,
				C
			) VALUES (
				?,
				?,
				?
			)
		`
		query = `
			SELECT
				A,
				B,
				C
			FROM clickhouse_test_uint64_support
		`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_uint64_support"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {
					var maxUint64 uint64 = 1<<64 - 1
					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {

						_, err = stmt.Exec(
							maxUint64,
							maxUint64-1,
							maxUint64-2,
						)
						if !assert.NoError(t, err) {
							return
						}

					}
					if assert.NoError(t, tx.Commit()) {
						var item struct {
							A uint64
							B uint64
							C uint64
						}
						if rows, err := connect.Query(query); assert.NoError(t, err) {

							for rows.Next() {
								err := rows.Scan(
									&item.A,
									&item.B,
									&item.C,
								)
								if !assert.NoError(t, err) {
									return
								}
							}
							assert.Equal(t, maxUint64, item.A)
							assert.Equal(t, maxUint64-1, item.B)
							assert.Equal(t, maxUint64-2, item.C)
						}
					}
				}
			}
		}
	}
}

func Test_Issue42_Plain_SQL_Support(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE dbr_people (
				id    UInt64,
				name  String,
				email String
			) Engine=Memory
		`
		dml   = "INSERT INTO `dbr_people` (`id`,`name`,`email`) VALUES (?, ?, ?)"
		query = "SELECT `id`,`name`,`email` FROM `dbr_people` WHERE `email` = 'jonathan@uservoice.com'"
	)

	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		if _, err := connect.Exec("DROP TABLE IF EXISTS `dbr_people`"); assert.NoError(t, err) {
			if _, err := connect.Exec(ddl); assert.NoError(t, err) {
				if tx, err := connect.Begin(); assert.NoError(t, err) {

					if stmt, err := tx.Prepare(dml); assert.NoError(t, err) {
						_, err = stmt.Exec(
							258,
							"jonathan",
							"jonathan@uservoice.com",
						)
						if !assert.NoError(t, err) {
							return
						}

					}
					if assert.NoError(t, tx.Commit()) {
						var item struct {
							ID    uint64
							Name  string
							Email string
						}
						if rows, err := connect.Query(query); assert.NoError(t, err) {

							for rows.Next() {
								err := rows.Scan(
									&item.ID,
									&item.Name,
									&item.Email,
								)
								if !assert.NoError(t, err) {
									return
								}
							}
							assert.Equal(t, uint64(258), item.ID)
							assert.Equal(t, "jonathan", item.Name)
							assert.Equal(t, "jonathan@uservoice.com", item.Email)
						}
					}
				}
			}
		}
	}
}

func TestBytes(t *testing.T) {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
	require.NoError(t, err)
	require.NoError(t, connect.Ping())
	defer connect.Close()

	_, err = connect.Exec(`DROP TABLE IF EXISTS TestBytes`)
	require.NoError(t, err)
	_, err = connect.Exec(`CREATE TABLE TestBytes (s String) Engine=Memory`)
	require.NoError(t, err)

	tx, err := connect.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = tx.Exec(`INSERT INTO TestBytes (s) VALUES (?)`, []byte("foo"))
	assert.NoError(t, err)
}
