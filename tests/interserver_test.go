package tests

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
)

// Values from tests/resources/custom.xml.
const (
	testClusterName   = "test_cluster_secret"
	testClusterSecret = "test_interserver_secret"
)

func TestInterserverSecretAuthenticatesAsInitialUser(t *testing.T) {
	SkipOnCloud(t, "cluster secret requires the tests/resources/custom.xml fixture")
	if RemoteClickHouse {
		t.Skip("cluster secret requires the tests/resources/custom.xml fixture")
	}
	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)

	admin, err := TestClientWithDefaultSettings(env)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	if !CheckMinServerServerVersion(admin, 23, 3, 0) {
		t.Skip("interserver-secret negotiation is exercised against >= 23.3 servers")
	}

	const initialUser = "interserver_test_user"
	createUser(t, admin, initialUser)
	t.Cleanup(func() { require.NoError(t, dropUser(admin, initialUser)) })

	timeout, err := strconv.Atoi(GetEnv("CLICKHOUSE_DIAL_TIMEOUT", "10"))
	require.NoError(t, err)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.Native,
		Addr:     []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: initialUser,
		},
		Cluster: clickhouse.ClusterCredentials{
			Name:   testClusterName,
			Secret: testClusterSecret,
		},
		DialTimeout: time.Duration(timeout) * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	queryID := fmt.Sprintf("interserver-test-%d", time.Now().UnixNano())
	ctx := clickhouse.Context(context.Background(),
		clickhouse.WithQueryID(queryID),
		clickhouse.WithInitialUser(initialUser),
	)

	var got uint8
	require.NoError(t, conn.QueryRow(ctx, "SELECT 42").Scan(&got))
	require.Equal(t, uint8(42), got)

	require.NoError(t, admin.Exec(context.Background(), "SYSTEM FLUSH LOGS"))

	var (
		loggedUser, loggedInitialUser string
		isInitialQuery                uint8
	)
	err = admin.QueryRow(context.Background(), `
		SELECT user, initial_user, is_initial_query
		FROM system.query_log
		WHERE query_id = ? AND type = 'QueryFinish'
		ORDER BY event_time_microseconds DESC
		LIMIT 1
	`, queryID).Scan(&loggedUser, &loggedInitialUser, &isInitialQuery)
	require.NoError(t, err, "expected query_log row for query_id=%s", queryID)

	require.Equal(t, initialUser, loggedUser, "server must run query as initial_user")
	require.Equal(t, initialUser, loggedInitialUser)
	require.Equal(t, uint8(0), isInitialQuery, "interserver-secret query must be Secondary (is_initial_query=0)")
}

func TestInterserverSecretWrongSecretRejected(t *testing.T) {
	SkipOnCloud(t, "cluster secret requires the tests/resources/custom.xml fixture")
	if RemoteClickHouse {
		t.Skip("cluster secret requires the tests/resources/custom.xml fixture")
	}
	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)

	admin, err := TestClientWithDefaultSettings(env)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	if !CheckMinServerServerVersion(admin, 23, 3, 0) {
		t.Skip("interserver-secret negotiation is exercised against >= 23.3 servers")
	}

	timeout, err := strconv.Atoi(GetEnv("CLICKHOUSE_DIAL_TIMEOUT", "10"))
	require.NoError(t, err)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.Native,
		Addr:     []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
		},
		Cluster: clickhouse.ClusterCredentials{
			Name:   testClusterName,
			Secret: "wrong-secret",
		},
		DialTimeout: time.Duration(timeout) * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx := clickhouse.Context(context.Background(),
		clickhouse.WithInitialUser(env.Username),
	)
	var got uint8
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&got)
	require.Error(t, err, "wrong cluster secret must produce a server-side error")
}

func TestInterserverSecretRejectsHTTPProtocol(t *testing.T) {
	_, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{"127.0.0.1:8123"},
		Auth:     clickhouse.Auth{Username: "default"},
		Cluster: clickhouse.ClusterCredentials{
			Name:   testClusterName,
			Secret: testClusterSecret,
		},
	})
	require.ErrorIs(t, err, clickhouse.ErrClusterSecretNeedsNative)
}

func TestInterserverSecretRequiresExplicitUsername(t *testing.T) {
	_, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.Native,
		Addr:     []string{"127.0.0.1:9000"},
		Cluster: clickhouse.ClusterCredentials{
			Name:   testClusterName,
			Secret: testClusterSecret,
		},
	})
	require.ErrorIs(t, err, clickhouse.ErrClusterSecretRequiresUsername)
}

func TestClusterCredentialsRedactSecret(t *testing.T) {
	c := clickhouse.ClusterCredentials{
		Name:   "my_cluster",
		Secret: "topsecret-do-not-print",
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		got := fmt.Sprintf(format, c)
		require.NotContains(t, got, "topsecret-do-not-print",
			"fmt %q must not leak Cluster.Secret value: got %q", format, got)
		require.Contains(t, got, "REDACTED",
			"fmt %q should mark the secret as REDACTED: got %q", format, got)
	}
}

func TestInterserverSecretRequiresClusterName(t *testing.T) {
	_, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.Native,
		Addr:     []string{"127.0.0.1:9000"},
		Auth:     clickhouse.Auth{Username: "default"},
		Cluster: clickhouse.ClusterCredentials{
			Secret: testClusterSecret,
		},
	})
	require.ErrorIs(t, err, clickhouse.ErrClusterSecretRequiresName)
}

func createUser(t *testing.T, admin interface {
	Exec(ctx context.Context, query string, args ...any) error
}, name string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, admin.Exec(ctx, fmt.Sprintf("DROP USER IF EXISTS %s", name)))
	require.NoError(t, admin.Exec(ctx, fmt.Sprintf(
		"CREATE USER %s IDENTIFIED WITH no_password",
		name,
	)))
	require.NoError(t, admin.Exec(ctx, fmt.Sprintf(
		"GRANT SELECT ON system.query_log TO %s", name,
	)))
}
