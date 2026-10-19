package clickhouse_api

import (
	"context"
	"fmt"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// ClusterSecretAuth runs a query as another user using a cluster secret.
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
