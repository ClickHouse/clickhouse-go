package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

const issue1854Setting = "output_format_native_use_flattened_dynamic_and_json_serialization"

type issue1854Rows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close() error
}

type issue1854Client struct {
	db        *sql.DB
	protocol  clickhouse.Protocol
	flattened bool
	scan      func(context.Context, string, ...any) error
	query     func(context.Context, string) (issue1854Rows, error)
	exec      func(context.Context, string) error
}

func test1854Clients(t *testing.T, mutate func(*clickhouse.Options), test func(*testing.T, issue1854Client)) {
	t.Helper()
	env, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		for _, api := range []string{"ClickHouse", "database_sql"} {
			t.Run(api, func(t *testing.T) {
				settings := clickhouse_tests.TestClientDefaultSettings(env)
				settings["allow_experimental_json_type"] = 1
				settings["allow_experimental_dynamic_type"] = 1
				settings["allow_experimental_variant_type"] = 1
				opts := clickhouse_tests.ClientOptionsFromEnv(env, settings, protocol == clickhouse.HTTP)
				opts.MaxOpenConns = 1
				opts.MaxIdleConns = 1
				if mutate != nil {
					mutate(&opts)
				}
				client := issue1854Client{protocol: protocol}
				if api == "ClickHouse" {
					conn, err := clickhouse.Open(&opts)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, conn.Close()) })
					if !clickhouse_tests.CheckMinServerServerVersion(conn, 24, 8, 0) {
						t.Skip("JSON and Dynamic require ClickHouse 24.8")
					}
					client.flattened = clickhouse_tests.CheckMinServerServerVersion(conn, 25, 6, 0)
					client.scan = func(ctx context.Context, query string, dest ...any) error {
						return conn.QueryRow(ctx, query).Scan(dest...)
					}
					client.query = func(ctx context.Context, query string) (issue1854Rows, error) { return conn.Query(ctx, query) }
					client.exec = func(ctx context.Context, query string) error { return conn.Exec(ctx, query) }
				} else {
					db := clickhouse.OpenDB(&opts)
					t.Cleanup(func() { require.NoError(t, db.Close()) })
					if !clickhouse_std_tests.CheckMinServerVersion(db, 24, 8, 0) {
						t.Skip("JSON and Dynamic require ClickHouse 24.8")
					}
					client.db = db
					client.flattened = clickhouse_std_tests.CheckMinServerVersion(db, 25, 6, 0)
					client.scan = func(ctx context.Context, query string, dest ...any) error {
						return db.QueryRowContext(ctx, query).Scan(dest...)
					}
					client.query = func(ctx context.Context, query string) (issue1854Rows, error) { return db.QueryContext(ctx, query) }
					client.exec = func(ctx context.Context, query string) error { _, err := db.ExecContext(ctx, query); return err }
				}
				test(t, client)
			})
		}
	})
}

func issue1854Admin(t *testing.T) driver.Conn {
	t.Helper()
	admin, err := clickhouse_tests.GetConnectionTCP(testSet, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	if !clickhouse_tests.CheckMinServerServerVersion(admin, 25, 6, 0) {
		t.Skip("flattened serialization requires ClickHouse 25.6")
	}
	return admin
}

func issue1854Context(value int) context.Context {
	return clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{issue1854Setting: value}))
}

func check1854Alive(t *testing.T, client issue1854Client) {
	t.Helper()
	var one uint8
	require.NoError(t, client.scan(context.Background(), "SELECT toUInt8(1)", &one))
	require.Equal(t, uint8(1), one)
}

func check1854Setting(t *testing.T, client issue1854Client, ctx context.Context, expected uint8) {
	t.Helper()
	var value uint8
	require.NoError(t, client.scan(ctx, "SELECT toUInt8(getSetting('"+issue1854Setting+"'))", &value))
	require.Equal(t, expected, value)
}

func check1854Overflow(t *testing.T, client issue1854Client, ctx context.Context, succeeds bool) {
	t.Helper()
	for _, tc := range []struct {
		name, query, storage string
		dynamic              bool
		count                int
	}{
		{"JSON", `SELECT '{"a":13,"b":"east"}'::JSON(max_dynamic_paths=0), 'tail'`, "JSON shared data", false, 1},
		{"JSON_Dynamic", `SELECT arrayJoin(['{"a":13}','{"a":"east"}'])::JSON(max_dynamic_types=1), 'tail'`, "Dynamic SharedVariant", false, 2},
		{"Dynamic", `SELECT toInt64(13)::Dynamic(max_types=0), 'tail'`, "Dynamic SharedVariant", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			read := func() error {
				rows, err := client.query(ctx, tc.query)
				if err != nil {
					return err
				}
				defer rows.Close()
				count := 0
				for rows.Next() {
					var tail string
					if tc.dynamic {
						var value chcol.Dynamic
						if err := rows.Scan(&value, &tail); err != nil {
							return err
						}
						require.Equal(t, int64(13), value.Any())
					} else {
						var value chcol.JSON
						if err := rows.Scan(&value, &tail); err != nil {
							return err
						}
						got, ok := chcol.ExtractJSONPathAsDynamic(&value, "a")
						require.True(t, ok)
						if count == 0 {
							require.Equal(t, int64(13), got.Any())
						} else {
							require.Equal(t, "east", got.Any())
						}
					}
					require.Equal(t, "tail", tail)
					count++
				}
				if err := rows.Err(); err != nil {
					return err
				}
				require.Equal(t, tc.count, count)
				return rows.Close()
			}
			err := read()
			if succeeds {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "cannot decode "+tc.storage)
			}
			check1854Alive(t, client)
		})
	}
}

func TestIssue1854Default(t *testing.T) {
	test1854Clients(t, nil, func(t *testing.T, client issue1854Client) {
		if client.flattened {
			check1854Setting(t, client, context.Background(), 1)
		}
		check1854Overflow(t, client, context.Background(), client.flattened)
	})
}

func TestIssue1854OptOut(t *testing.T) {
	test1854Clients(t, func(opts *clickhouse.Options) { opts.DisableAutoFlattenedJSONOutput = true }, func(t *testing.T, client issue1854Client) {
		if client.flattened {
			check1854Setting(t, client, context.Background(), 0)
		}
		check1854Overflow(t, client, context.Background(), false)
	})
}

func TestIssue1854Settings(t *testing.T) {
	issue1854Admin(t)
	for _, scope := range []string{"connection", "query", "opt_out"} {
		for _, value := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s_%d", scope, value), func(t *testing.T) {
				test1854Clients(t, func(opts *clickhouse.Options) {
					if scope == "connection" {
						opts.Settings[issue1854Setting] = value
					}
					opts.DisableAutoFlattenedJSONOutput = scope == "opt_out"
				}, func(t *testing.T, client issue1854Client) {
					if !client.flattened {
						t.Skip("flattened serialization requires ClickHouse 25.6")
					}
					ctx := context.Background()
					if scope != "connection" {
						ctx = issue1854Context(value)
					}
					check1854Setting(t, client, ctx, uint8(value))
					check1854Overflow(t, client, ctx, value == 1)
					check1854Setting(t, client, issue1854Context(1-value), uint8(1-value))
					check1854Overflow(t, client, issue1854Context(1-value), value == 0)
				})
			})
		}
	}
}

func TestIssue1854ReadonlySetting(t *testing.T) {
	test1854Clients(t, func(opts *clickhouse.Options) {
		opts.Settings = clickhouse.Settings{"readonly": 1}
		opts.DialTimeout = 0
	}, func(t *testing.T, client issue1854Client) {
		check1854Alive(t, client)
		if client.flattened {
			check1854Setting(t, client, context.Background(), 1)
		}
		check1854Overflow(t, client, context.Background(), client.flattened)
	})
}

func TestIssue1854Profiles(t *testing.T) {
	ctx := context.Background()
	admin := issue1854Admin(t)
	for _, tc := range []struct {
		name, settings string
		enabled        bool
		ordinaryOnly   bool
	}{
		{"readonly_one", "readonly=1, " + issue1854Setting + "=0", false, false},
		{"readonly_one_flattened", "readonly=1, " + issue1854Setting + "=1", true, false},
		{"readonly_two", "readonly=2, " + issue1854Setting + "=0", true, false},
		{"changeable_in_readonly", "readonly=1, " + issue1854Setting + "=0 CHANGEABLE_IN_READONLY", true, false},
		{"locked", issue1854Setting + "=0 READONLY", false, false},
		{"max_zero", issue1854Setting + "=0 MAX 0", false, false},
		{"max_one", issue1854Setting + "=0 MAX 1", true, false},
		{"query_cache", "use_query_cache=1, query_cache_nondeterministic_function_handling='ignore'", false, true},
		{"max_columns", "max_columns_to_read=2", false, true},
		{"max_memory", "max_memory_usage=1000000", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			username := "issue1854_" + clickhouse_tests.RandAsciiString(8)
			password := clickhouse_tests.RandAsciiString(16) + "1#"
			require.NoError(t, admin.Exec(ctx, fmt.Sprintf("CREATE USER %s IDENTIFIED BY '%s' SETTINGS %s", username, password, tc.settings)))
			require.NoError(t, admin.Exec(ctx, "GRANT SELECT ON *.* TO "+username))
			t.Cleanup(func() { require.NoError(t, admin.Exec(ctx, "DROP USER IF EXISTS "+username)) })
			test1854Clients(t, func(opts *clickhouse.Options) {
				opts.Auth.Username, opts.Auth.Password, opts.Settings = username, password, nil
			}, func(t *testing.T, client issue1854Client) {
				check1854Alive(t, client)
				var expected uint8
				if tc.enabled {
					expected = 1
				}
				if tc.ordinaryOnly {
					var number uint8
					var text string
					require.NoError(t, client.scan(ctx, "SELECT toUInt8(13), 'east'", &number, &text))
					require.Equal(t, uint8(13), number)
					require.Equal(t, "east", text)
					return
				}
				check1854Setting(t, client, ctx, expected)
				var plain chcol.JSON
				require.NoError(t, client.scan(ctx, `SELECT '{"a":13}'::JSON`, &plain))
				check1854Overflow(t, client, ctx, tc.enabled)
			})
		})
	}
}

func TestIssue1854SessionRecovery(t *testing.T) {
	if clickhouse_tests.CloudClickHouse {
		t.Skip("requires a session on one replica")
	}
	test1854Clients(t, func(opts *clickhouse.Options) {
		opts.Settings = clickhouse.Settings{}
		opts.DialTimeout = 0
		opts.Compression = nil
		if opts.Protocol == clickhouse.HTTP {
			opts.Settings["session_id"] = "issue1854_" + clickhouse_tests.RandAsciiString(16)
		}
	}, func(t *testing.T, client issue1854Client) {
		if !client.flattened {
			t.Skip("flattened serialization requires ClickHouse 25.6")
		}
		ctx := context.Background()
		if client.db != nil && client.protocol == clickhouse.HTTP {
			conn, err := client.db.Conn(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			client.scan = func(ctx context.Context, query string, dest ...any) error {
				return conn.QueryRowContext(ctx, query).Scan(dest...)
			}
			client.exec = func(ctx context.Context, query string) error { _, err := conn.ExecContext(ctx, query); return err }
		}
		check1854Setting(t, client, ctx, 1)
		require.NoError(t, client.exec(ctx, "SET readonly=1"))
		var value uint8
		var exception *clickhouse.Exception
		require.ErrorAs(t, client.scan(ctx, "SELECT toUInt8(1)", &value), &exception)
		require.EqualValues(t, 164, exception.Code)
		check1854Alive(t, client)
		require.NoError(t, client.scan(ctx, "SELECT toUInt8(getSetting('readonly'))", &value))
		// Some servers close a Native connection after the rejection.
		if client.protocol == clickhouse.Native && (client.db == nil || value == 0) {
			require.Zero(t, value, "a new Native connection has a fresh session")
			check1854Setting(t, client, ctx, 1)
		} else {
			require.Equal(t, uint8(1), value)
			check1854Setting(t, client, ctx, 0)
			var plain chcol.JSON
			require.NoError(t, client.scan(ctx, `SELECT '{"a":13}'::JSON`, &plain))
			require.ErrorContains(t, client.scan(ctx, `SELECT '{"a":13}'::JSON(max_dynamic_paths=0)`, &plain), "cannot decode JSON shared data")
		}
	})
}

func TestIssue1854RejectedConnectionSetting(t *testing.T) {
	issue1854Admin(t)
	env, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := clickhouse_tests.ClientOptionsFromEnv(env, clickhouse.Settings{"zz_no_such_setting": 1}, false)
	ctx := context.Background()
	for _, api := range []string{"ClickHouse", "database_sql"} {
		t.Run(api, func(t *testing.T) {
			var err error
			var value uint8
			if api == "ClickHouse" {
				conn, openErr := clickhouse.Open(&opts)
				require.NoError(t, openErr)
				t.Cleanup(func() { require.NoError(t, conn.Close()) })
				require.NoError(t, conn.Ping(ctx))
				err = conn.QueryRow(ctx, "SELECT toUInt8(1)").Scan(&value)
			} else {
				db := clickhouse.OpenDB(&opts)
				t.Cleanup(func() { require.NoError(t, db.Close()) })
				require.NoError(t, db.PingContext(ctx))
				err = db.QueryRowContext(ctx, "SELECT toUInt8(1)").Scan(&value)
			}
			var exception *clickhouse.Exception
			require.ErrorAs(t, err, &exception)
			require.EqualValues(t, 115, exception.Code)
		})
	}
}
