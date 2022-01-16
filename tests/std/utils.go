package std

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}
func checkMinServerVersion(conn *sql.DB, major, minor uint64) error {
	var version struct {
		Major uint64
		Minor uint64
	}
	var res string
	if err := conn.QueryRow("SELECT version()").Scan(&res); err != nil {
		panic(err)
	}
	for i, v := range strings.Split(res, ".") {
		switch i {
		case 0:
			version.Major, _ = strconv.ParseUint(v, 10, 64)
		case 1:
			version.Minor, _ = strconv.ParseUint(v, 10, 64)
		}
	}
	if version.Major < major || (version.Major == major && version.Minor < minor) {
		return fmt.Errorf("unsupported server version %d.%d < %d.%d", version.Major, version.Minor, major, minor)
	}
	return nil
}
