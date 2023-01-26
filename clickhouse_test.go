package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnOptions(t *testing.T) {
	dsn := "clickhouse://127.0.0.1/test_database?secure=true"
	opts, err := ParseDSN(dsn)
	require.NoError(t, err)
	conn, err := Open(opts)
	require.NoError(t, err)

	got := conn.Options()
	// Check that conn returned shallow copy of Options.
	require.NotSame(t, opts, got)
	// Set defaults to pass the assertion.
	opts = opts.setDefaults()
	require.Equal(t, opts, got)

	require.NoError(t, conn.Close())
}
