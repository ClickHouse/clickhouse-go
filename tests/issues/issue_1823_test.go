package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test1823 verifies that a typed nil pointer to a type with a value receiver Value method,
// such as (*uuid.UUID)(nil), binds as NULL and does not panic. It covers the positional,
// numeric and named placeholders.
// See https://github.com/ClickHouse/clickhouse-go/issues/1823.
func Test1823(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	var typedNil *uuid.UUID
	cases := []struct {
		name, query string
		args        []any
	}{
		{"positional", "SELECT ?", []any{typedNil}},
		{"numeric", "SELECT $1", []any{typedNil}},
		{"named", "SELECT @v", []any{clickhouse.Named("v", typedNil)}},
	}
	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprint(protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					// The destination starts as a non-nil pointer, so the test fails if the
					// NULL result does not reset it. UUID.Scan(nil) does nothing, so a
					// **uuid.UUID destination is the one that shows the NULL.
					seed := uuid.New()
					got := &seed
					var err error
					require.NotPanics(t, func() {
						err = conn.QueryRow(context.Background(), tc.query, tc.args...).Scan(&got)
					})
					require.NoError(t, err)
					require.Nil(t, got)
				})
			}
		})
		t.Run("std_"+fmt.Sprint(protocol), func(t *testing.T) {
			opts := clickhouse_tests.ClientOptionsFromEnv(testEnv, nil, protocol == clickhouse.HTTP)
			db, err := sql.Open("clickhouse", clickhouse_tests.OptionsToDSN(&opts))
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					seed := uuid.New()
					got := &seed
					var err error
					require.NotPanics(t, func() {
						err = db.QueryRowContext(context.Background(), tc.query, tc.args...).Scan(&got)
					})
					require.NoError(t, err)
					require.Nil(t, got)
				})
			}
		})
	}
}
