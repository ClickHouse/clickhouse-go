// +build go1.8

package clickhouse

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Context_Timeout(t *testing.T) {
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
			defer cancel()
			if _, err := connect.QueryContext(ctx, "SELECT 1, sleep(10)"); assert.Error(t, err) {
				assert.Equal(t, context.DeadlineExceeded, err)
			}
		}
		{
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if rows, err := connect.QueryContext(ctx, "SELECT 1, sleep(0.1)"); assert.NoError(t, err) {
				if assert.True(t, rows.Next()) {
					var value, value2 int
					if assert.NoError(t, rows.Scan(&value, &value2)) {
						assert.Equal(t, int(1), value)
					}
				}
			}
		}
	}
}
