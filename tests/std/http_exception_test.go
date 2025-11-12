package std

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPExceptionHandlingDB(t *testing.T) {
	conn, err := GetStdOpenDBConnection(clickhouse.HTTP, nil, nil, nil)
	require.NoError(t, err)

	ctx := context.Background()

	// These settings will make sure mid-stream exception most likely on the server
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"max_threads":                           1,
		"max_block_size":                        1,
		"http_write_exception_in_output_format": 0,
		"wait_end_of_query":                     0,
		"http_response_buffer_size":             1,
	}))

	rows, err := conn.QueryContext(ctx, `SELECT throwIf(number=3, 'there is an exception') FROM system.numbers`)
	require.NoError(t, err) // query shouldn't fail with 500 status code.

	occured := false
	// query should fail while scanning the rows mid-stream
	for rows.Next() {
		var result uint8
		err := rows.Scan(&result)
		if err != nil {
			// should be an exception caught correctly
			assert.Contains(t, err.Error(), "there is an exception", "Expected exception message not caught")
			occured = true
		}
	}

	if err := rows.Err(); err != nil {
		assert.Contains(t, err.Error(), "there is an exception", "Expected exception message not caught")
		occured = true
	}

	assert.True(t, occured, "execption not caught in the response chunks")
}
