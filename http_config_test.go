package clickhouse

import (
	"fmt"
	"testing"
)

func TestParseHTTPDsn(t *testing.T) {
	dsn := "http://username:password@127.0.0.1:9000/test?dial_timeout=1s&compress=true"

	c, err := parseHttpPDsn(dsn)

	fmt.Println(c)
	fmt.Println(err)

}
