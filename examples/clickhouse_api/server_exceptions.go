package clickhouse_api

import (
	"context"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ServerExceptions shows how to branch on server-side failures. A typed
// *clickhouse.Exception is available on both the native and HTTP protocols;
// over HTTP, a non-200 response is additionally wrapped in
// *clickhouse.HTTPError carrying the status code.
func ServerExceptions() error {
	native, err := GetNativeConnection(nil, nil, nil)
	if err != nil {
		return err
	}
	defer native.Close()
	http, err := GetHTTPConnection("server-exceptions-example", nil, nil, nil)
	if err != nil {
		return err
	}
	defer http.Close()

	ctx := context.Background()

	for _, conn := range []driver.Conn{native, http} {
		_, err := conn.Query(ctx, "SELECT * FROM non_existent_table")

		// The same check works regardless of protocol.
		var chErr *clickhouse.Exception
		if !errors.As(err, &chErr) {
			return fmt.Errorf("expected *clickhouse.Exception, got: %w", err)
		}
		if chErr.Code != 60 { // UNKNOWN_TABLE
			return fmt.Errorf("expected code 60, got %d", chErr.Code)
		}
		// CodeName is the symbolic name for Code, e.g. "UNKNOWN_TABLE".
		// Best-effort: currently only set over HTTP.
		fmt.Printf("server exception: code=%d codeName=%s name=%s\n", chErr.Code, chErr.CodeName, chErr.Name)

		// HTTP only: the status code of the non-200 response. Exceptions that
		// occur after the server started streaming a 200 are not wrapped —
		// they surface as a bare *clickhouse.Exception on both protocols.
		var httpErr *clickhouse.HTTPError
		if errors.As(err, &httpErr) {
			fmt.Printf("http status: %d\n", httpErr.StatusCode)
		}
	}
	return nil
}
