package tests

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"path"
	"strings"
	"testing"
)

const defaultClickHouseVersion = "latest"

func GetClickHouseTestVersion() string {
	return GetEnv("CLICKHOUSE_VERSION", defaultClickHouseVersion)
}

func TestMain(m *testing.M) {
	useDocker := strings.ToLower(GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if useDocker == "false" {
		fmt.Printf("Using external ClickHouse for IT tests -  %s:%s\n",
			GetEnv("CLICKHOUSE_PORT", "9000"),
			GetEnv("CLICKHOUSE_HOST", "localhost"))
		os.Exit(m.Run())
	}

	// create a ClickHouse container
	ctx := context.Background()
	// attempt use docker for CI
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		fmt.Printf("Docker is not running and no clickhouse connections details were provided. Skipping tests: %s\n", err)
		os.Exit(0)
	}
	err = provider.Health(ctx)
	if err != nil {
		fmt.Printf("Docker is not running and no clickhouse connections details were provided. Skipping IT tests: %s\n", err)
		os.Exit(0)
	}
	fmt.Printf("Using Docker for IT tests\n")
	cwd, err := os.Getwd()
	if err != nil {
		// can't test without container
		panic(err)
	}
	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("clickhouse/clickhouse-server:%s", GetClickHouseTestVersion()),
		ExposedPorts: []string{"9000/tcp", "8123/tcp", "9440/tcp", "8443/tcp"},
		WaitingFor:   wait.ForLog("Ready for connections"),
		Mounts: []testcontainers.ContainerMount{
			testcontainers.BindMount(path.Join(cwd, "./resources/custom.xml"), "/etc/clickhouse-server/config.d/custom.xml"),
			testcontainers.BindMount(path.Join(cwd, "./resources/admin.xml"), "/etc/clickhouse-server/users.d/admin.xml"),
			testcontainers.BindMount(path.Join(cwd, "./resources/clickhouse.crt"), "/etc/clickhouse-server/certs/clickhouse.crt"),
			testcontainers.BindMount(path.Join(cwd, "./resources/clickhouse.key"), "/etc/clickhouse-server/certs/clickhouse.key"),
			testcontainers.BindMount(path.Join(cwd, "./resources/CAroot.crt"), "/etc/clickhouse-server/certs/CAroot.crt"),
		},
	}
	clickhouseContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		// can't test without container
		panic(err)
	}

	p, _ := clickhouseContainer.MappedPort(ctx, "9000")
	os.Setenv("CLICKHOUSE_PORT", p.Port())
	hp, _ := clickhouseContainer.MappedPort(ctx, "8123")
	os.Setenv("CLICKHOUSE_HTTP_PORT", hp.Port())
	sslPort, _ := clickhouseContainer.MappedPort(ctx, "9440")
	os.Setenv("CLICKHOUSE_SSL_PORT", sslPort.Port())
	hps, _ := clickhouseContainer.MappedPort(ctx, "8443")
	os.Setenv("CLICKHOUSE_HTTPS_PORT", hps.Port())
	os.Setenv("CLICKHOUSE_HOST", "localhost")
	// we set this explicitly - note its also set in the /etc/clickhouse-server/users.d/admin.xml
	os.Setenv("CLICKHOUSE_PASSWORD", "ClickHouse")
	defer clickhouseContainer.Terminate(ctx) //nolint
	os.Exit(m.Run())
}
