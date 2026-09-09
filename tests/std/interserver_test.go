package std

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestInterserverSecretValidation(t *testing.T) {
	tests := []struct {
		name string
		opt  *clickhouse.Options
		want error
	}{
		{
			name: "requires explicit username",
			opt: &clickhouse.Options{Cluster: clickhouse.ClusterCredentials{
				Name: "cluster", Secret: "secret",
			}},
			want: clickhouse.ErrClusterSecretRequiresUsername,
		},
		{
			name: "requires cluster name",
			opt: &clickhouse.Options{
				Auth:    clickhouse.Auth{Username: "user"},
				Cluster: clickhouse.ClusterCredentials{Secret: "secret"},
			},
			want: clickhouse.ErrClusterSecretRequiresName,
		},
		{
			name: "requires native protocol",
			opt: &clickhouse.Options{
				Protocol: clickhouse.HTTP,
				Auth:     clickhouse.Auth{Username: "user"},
				Cluster:  clickhouse.ClusterCredentials{Name: "cluster", Secret: "secret"},
			},
			want: clickhouse.ErrClusterSecretNeedsNative,
		},
		{
			name: "rejects JWT",
			opt: &clickhouse.Options{
				Auth:    clickhouse.Auth{Username: "user"},
				Cluster: clickhouse.ClusterCredentials{Name: "cluster", Secret: "secret"},
				GetJWT: func(context.Context) (string, error) {
					return "token", nil
				},
			},
			want: clickhouse.ErrClusterSecretWithJWT,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("Connector", func(t *testing.T) {
				conn, err := clickhouse.Connector(tc.opt).Connect(context.Background())
				require.Nil(t, conn)
				require.ErrorIs(t, err, tc.want)
			})

			t.Run("OpenDB", func(t *testing.T) {
				db := clickhouse.OpenDB(tc.opt)
				err := db.PingContext(context.Background())
				require.ErrorIs(t, err, tc.want)
				require.NoError(t, db.Close())
			})
		})
	}
}
