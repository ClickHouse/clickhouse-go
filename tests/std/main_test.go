package std

import (
	"crypto/tls"
	"database/sql"
	"net/url"
	"os"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

const testSet string = "std"

func TestMain(m *testing.M) {
	os.Exit(clickhouse_tests.Runtime(m, testSet))
}

func GetStdDSNConnection(protocol clickhouse.Protocol, secure bool, opts url.Values) (*sql.DB, error) {
	return GetDSNConnection(testSet, protocol, secure, opts)
}

func GetStdOpenDBConnection(protocol clickhouse.Protocol, settings clickhouse.Settings, tlsConfig *tls.Config, compression *clickhouse.Compression) (*sql.DB, error) {
	return GetOpenDBConnection(testSet, protocol, settings, tlsConfig, compression)
}
