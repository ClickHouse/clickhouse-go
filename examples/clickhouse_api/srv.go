package clickhouse_api

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// ResolveSRVVersion opens a connection whose addresses are DNS SRV names
// resolved on every new connection. The example uses the full SRV form
// `_clickhouse._tcp.example.com`; a plain host would also work and would be
// looked up as `_clickhouse._tcp.<host>` for the native protocol.
func ResolveSRVVersion() error {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:       []string{"_clickhouse._tcp.example.com"},
		ResolveSRV: true,
	})
	if err != nil {
		return err
	}
	defer conn.Close()
	v, err := conn.ServerVersion()
	if err != nil {
		return err
	}
	fmt.Println(v.String())
	return conn.Ping(context.Background())
}
