package clickhouse_api

import (
	"context"
	"fmt"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// ClusterSecretAuth shows how to authenticate as a trusted cluster peer
// using ClickHouse's interserver shared secret instead of a user password,
// then run a query as an arbitrary `initial_user` chosen per-call.
//
// Two prerequisites on the server:
//
//  1. <remote_servers><my_cluster><secret>...</secret>...</my_cluster></remote_servers>
//     in the ClickHouse config — the same secret value that the client supplies.
//  2. The user named in WithInitialUser must exist on the server; the server
//     runs the query as that user without a password check, since the cluster
//     secret signature already proves the caller is trusted.
//
// The secret is sensitive cluster-wide credential material. Source it from
// an environment variable, secret manager, or sealed config — never hard-code
// it and never embed it in a DSN string (DSNs end up in logs and stack traces).
func ClusterSecretAuth() error {
	env, err := GetNativeTestEnvironment()
	if err != nil {
		return err
	}

	secret := os.Getenv("CLICKHOUSE_CLUSTER_SECRET")
	if secret == "" {
		return fmt.Errorf("CLICKHOUSE_CLUSTER_SECRET must be set")
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
		},
		Cluster: clickhouse.ClusterCredentials{
			Name:   "my_cluster",
			Secret: secret,
		},
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx := clickhouse.Context(context.Background(),
		clickhouse.WithInitialUser("alice"),
	)

	var got int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&got); err != nil {
		return err
	}
	fmt.Printf("query ran as alice via interserver-secret auth, result=%d\n", got)
	return nil
}
