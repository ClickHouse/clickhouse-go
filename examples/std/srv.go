package std

import (
	"database/sql"
)

// ResolveSRVDSN opens a database/sql handle whose address is a DNS SRV name
// resolved on every new connection.
func ResolveSRVDSN() error {
	db, err := sql.Open("clickhouse", "clickhouse://_clickhouse._tcp.example.com?resolve_srv=true")
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Ping()
}
