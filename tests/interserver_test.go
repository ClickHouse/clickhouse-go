package tests

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/require"
)

// testClusterSecret matches the <secret> configured for <test_cluster_secret>
// in tests/resources/custom.xml. Hard-coded because the cluster name and the
// secret are server-side configuration; the test only needs to mirror them.
const (
	testClusterName   = "test_cluster_secret"
	testClusterSecret = "test_interserver_secret"
)

// TestInterserverSecretAuthenticatesAsInitialUser verifies the end-to-end
// interserver-secret flow: a connection that authenticates with the cluster
// secret can run a query as a non-default user without supplying that user's
// password, and the query is logged in `system.query_log` as Secondary.
func TestInterserverSecretAuthenticatesAsInitialUser(t *testing.T) {
	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)

	if !CheckMinClickHouseVersion(t, env, 23, 3, 0) {
		t.Skip("interserver-secret negotiation is exercised against >= 23.3 servers")
	}

	admin, err := TestClientWithDefaultSettings(env)
	require.NoError(t, err)
	defer admin.Close()

	const initialUser = "interserver_test_user"
	createUser(t, admin, initialUser)
	defer dropUserBestEffort(admin, initialUser)

	timeout, _ := strconv.Atoi(GetEnv("CLICKHOUSE_DIAL_TIMEOUT", "10"))
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
	defer conn.Close()

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

// TestInterserverSecretWrongSecretRejected verifies the server refuses a
// query signed with a wrong cluster secret — the negative path is essential
// to confirm the signature is actually being checked, not silently ignored.
func TestInterserverSecretWrongSecretRejected(t *testing.T) {
	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)

	if !CheckMinClickHouseVersion(t, env, 23, 3, 0) {
		t.Skip("interserver-secret negotiation is exercised against >= 23.3 servers")
	}

	timeout, _ := strconv.Atoi(GetEnv("CLICKHOUSE_DIAL_TIMEOUT", "10"))
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
	defer conn.Close()

	ctx := clickhouse.Context(context.Background(),
		clickhouse.WithInitialUser(env.Username),
	)
	var got uint8
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&got)
	require.Error(t, err, "wrong cluster secret must produce a server-side error")
}

// TestInterserverSecretRejectsHTTPProtocol guards the validation path
// added in clickhouse.Open: HTTP + Cluster.Secret is a misconfiguration we
// catch up front rather than silently downgrading to password auth.
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

// TestInterserverSecretRequiresExplicitUsername closes the implicit-default
// footgun: a caller who sets Cluster.Secret but leaves Auth.Username blank
// would otherwise have setDefaults silently rewrite Auth.Username to "default",
// and a forgotten WithInitialUser would then run queries as the cluster
// superuser. Open must refuse this configuration.
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

// TestClusterCredentialsRedactSecret verifies that fmt.Sprintf and friends
// cannot leak Cluster.Secret. A future contributor adding slog.Any("opt", opt)
// or fmt.Printf("%+v", opt) anywhere in the code must not turn that into a
// credential leak.
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

// TestInterserverSecretRequiresClusterName guards the second validation
// branch: a non-empty Secret with an empty Name cannot succeed at handshake
// time, so we reject it at Open() rather than letting the server return an
// opaque error.
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

// CheckMinClickHouseVersion compares against the env's recorded version. We
// inline this rather than reuse CheckMinServerServerVersion because the
// latter requires an open connection and we want a cheap pre-flight check.
func CheckMinClickHouseVersion(t *testing.T, env ClickHouseTestEnvironment, major, minor, patch uint64) bool {
	t.Helper()
	return proto.CheckMinVersion(proto.Version{Major: major, Minor: minor, Patch: patch}, env.Version)
}

func createUser(t *testing.T, admin interface {
	Exec(ctx context.Context, query string, args ...any) error
}, name string) {
	t.Helper()
	ctx := context.Background()
	_ = admin.Exec(ctx, fmt.Sprintf("DROP USER IF EXISTS %s", name))
	require.NoError(t, admin.Exec(ctx, fmt.Sprintf(
		"CREATE USER %s IDENTIFIED WITH no_password",
		name,
	)))
	require.NoError(t, admin.Exec(ctx, fmt.Sprintf(
		"GRANT SELECT ON system.query_log TO %s", name,
	)))
}

func dropUserBestEffort(admin interface {
	Exec(ctx context.Context, query string, args ...any) error
}, name string) {
	_ = admin.Exec(context.Background(), fmt.Sprintf("DROP USER IF EXISTS %s", name))
}
