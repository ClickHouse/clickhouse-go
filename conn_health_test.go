package clickhouse

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func healthyMockConnect() *connect {
	conn := createMockConnect(&mockNetConn{})
	conn.connectedAt = time.Now()
	conn.opt = &Options{ConnMaxLifetime: time.Hour}
	return conn
}

func TestConnectHealthCheck_Healthy(t *testing.T) {
	conn := healthyMockConnect()

	assert.NoError(t, conn.healthCheck())
}

func TestConnectHealthCheck_Closed(t *testing.T) {
	conn := healthyMockConnect()
	conn.setClosed()

	err := conn.healthCheck()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConnectionClosed)
}

func TestConnectHealthCheck_LifetimeExceeded(t *testing.T) {
	conn := healthyMockConnect()
	conn.connectedAt = time.Now().Add(-2 * time.Hour)

	err := conn.healthCheck()
	require.Error(t, err)
	assert.ErrorIs(t, err, errConnMaxLifetimeExceeded)
	assert.Contains(t, err.Error(), "age")
	assert.Contains(t, err.Error(), "max lifetime")
}

func TestHTTPConnectHealthCheck(t *testing.T) {
	closed := &httpConnect{}
	err := closed.healthCheck()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConnectionClosed)

	healthy := &httpConnect{client: &http.Client{}}
	assert.NoError(t, healthy.healthCheck())
}
