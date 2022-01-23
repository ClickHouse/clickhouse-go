package issues

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}
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
