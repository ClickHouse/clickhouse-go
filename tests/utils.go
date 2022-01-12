package tests

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func checkMinServerVersion(conn driver.Conn, major, minor uint64) error {
	v, err := conn.ServerVersion()
	if err != nil {
		panic(err)
	}
	if v.Version.Major < major || (v.Version.Major == major && v.Version.Minor < minor) {
		return fmt.Errorf("unsupported server version %d.%d < %d.%d", v.Version.Major, v.Version.Minor, major, minor)
	}
	return nil
}
