package clickhouse_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Server-side failures carry a typed *clickhouse.Exception on both the native
// and HTTP protocols. Over HTTP, a non-200 response is additionally wrapped in
// *clickhouse.HTTPError; exceptions that occur after the server started
// streaming a 200 response surface as a bare *clickhouse.Exception.
func ExampleHTTPError() {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{"localhost:8123"},
	})
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	_, err = conn.Query(context.Background(), "SELECT * FROM non_existent_table")

	var chErr *clickhouse.Exception
	if errors.As(err, &chErr) {
		// works on the native protocol too; CodeName (e.g. "UNKNOWN_TABLE")
		// is best-effort and currently only set over HTTP
		fmt.Println(chErr.Code, chErr.CodeName, chErr.Message)
	}

	var httpErr *clickhouse.HTTPError
	if errors.As(err, &httpErr) {
		fmt.Println(httpErr.StatusCode)
	}
}
