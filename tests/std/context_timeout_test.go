package std

import (
	"context"
	"database/sql"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Context_Timeout(t *testing.T) {
	if connect, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
			defer cancel()
			if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(1)"); assert.NotNil(t, row) {
				var a, b int
				if err := row.Scan(&a, &b); assert.Error(t, err) {
					switch err := err.(type) {
					case *net.OpError:
						assert.Equal(t, "read", err.Op)
					default:
						assert.Equal(t, context.DeadlineExceeded, err)
					}
				}
			}
		}
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(0.1)"); assert.NotNil(t, row) {
				var value, value2 int
				if assert.NoError(t, row.Scan(&value, &value2)) {
					assert.Equal(t, int(1), value)
				}
			}
		}
	}
}
