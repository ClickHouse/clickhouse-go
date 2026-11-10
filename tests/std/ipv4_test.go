package std

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"

	"github.com/stretchr/testify/assert"
)

func TestStdIPv4(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)

			const ddl = `
			CREATE TABLE std_test_ipv4 (
				  Col1 IPv4
				, Col2 IPv4
				, Col3 Nullable(IPv4)
				, Col4 Array(IPv4)
				, Col5 Array(Nullable(IPv4))
			) Engine MergeTree() ORDER BY tuple()
		`
			defer func() {
				conn.Exec("DROP TABLE std_test_ipv4")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO std_test_ipv4")
			require.NoError(t, err)
			var (
				col1Data = net.ParseIP("127.0.0.1")
				col2Data = net.ParseIP("8.8.8.8")
				col3Data = col1Data
				col4Data = []net.IP{col1Data, col2Data}
				col5Data = []*net.IP{&col1Data, nil, &col2Data}
			)
			_, err = batch.Exec(col1Data, col2Data, &col3Data, &col4Data, &col5Data)
			require.NoError(t, err)
			require.NoError(t, scope.Commit())
			var (
				col1 net.IP
				col2 net.IP
				col3 *net.IP
				col4 []net.IP
				col5 []*net.IP
			)
			require.NoError(t, conn.QueryRow("SELECT * FROM std_test_ipv4").Scan(&col1, &col2, &col3, &col4, &col5))
			assert.Equal(t, col1Data.To4(), col1)
			assert.Equal(t, col2Data.To4(), col2)
			assert.Equal(t, col3Data.To4(), *col3)
			require.Len(t, col4, 2)
			assert.Equal(t, col1Data.To4(), col4[0])
			assert.Equal(t, col2Data.To4(), col4[1])
			require.Len(t, col5, 3)
			require.Nil(t, col5[1])
			assert.Equal(t, col1Data.To4(), *col5[0])
			assert.Equal(t, col2Data.To4(), *col5[2])
		})
	}
}

func TestStdIPv4RejectsIPv6AndAcceptsMappedIPv6(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			const ddl = `
			CREATE TABLE std_test_ipv4_mapped (
				  Col1 IPv4
				, Col2 IPv4
			) Engine MergeTree() ORDER BY tuple()
		`
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, err := conn.Exec("DROP TABLE IF EXISTS std_test_ipv4_mapped")
				require.NoError(t, err)
			})

			const ipv6 = "2001:db8::1"
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO std_test_ipv4_mapped")
			require.NoError(t, err)
			_, err = batch.Exec(net.ParseIP(ipv6), net.ParseIP("192.0.2.2"))
			var convErr *column.ColumnConverterError
			require.ErrorAs(t, err, &convErr, "input %s: expected a column converter error", ipv6)
			require.ErrorContains(t, err, ipv6)
			require.NoError(t, scope.Rollback())

			const mapped = "::ffff:192.0.2.1"
			scope, err = conn.Begin()
			require.NoError(t, err)
			batch, err = scope.Prepare("INSERT INTO std_test_ipv4_mapped")
			require.NoError(t, err)
			_, err = batch.Exec(net.ParseIP(mapped), netip.MustParseAddr(mapped))
			require.NoError(t, err)
			require.NoError(t, scope.Commit())

			var col1, col2 string
			require.NoError(t, conn.QueryRow("SELECT toString(Col1), toString(Col2) FROM std_test_ipv4_mapped").Scan(&col1, &col2))
			assert.Equal(t, "192.0.2.1", col1)
			assert.Equal(t, "192.0.2.1", col2)
		})
	}
}
