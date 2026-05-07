//go:build docker

package tests

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type ClickHouseTestEnvironment struct {
	ContainerID string
	Port        int
	HttpPort    int
	SslPort     int
	HttpsPort   int
	Host        string
	Username    string
	Password    string
	JWT         string
	Database    string
	Version     proto.Version
	ContainerIP string
	Container   testcontainers.Container `json:"-"`
}

func (env *ClickHouseTestEnvironment) setVersion() {
	useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
	if err != nil {
		panic(err)
	}
	port := env.Port
	var tlsConfig *tls.Config
	if useSSL {
		tlsConfig = &tls.Config{}
		port = env.SslPort
	}
	timeout, err := strconv.Atoi(GetEnv("CLICKHOUSE_DIAL_TIMEOUT", "10"))
	if err != nil {
		panic(err)
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Settings: nil,
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		TLS:         tlsConfig,
		DialTimeout: time.Duration(timeout) * time.Second,
	})
	if err != nil {
		panic(err)
	}
	v, err := conn.ServerVersion()
	if err != nil {
		panic(err)
	}
	env.Version = v.Version
}

func CreateClickHouseTestEnvironment(testSet string) (ClickHouseTestEnvironment, error) {
	// create a ClickHouse Container
	ctx := context.Background()
	// attempt use docker for CI
	provider, err := testcontainers.ProviderDefault.GetProvider()
	if err != nil {
		fmt.Printf("Docker is not running and no clickhouse connections details were provided. Skipping tests: %s\n", err)
		os.Exit(0)
	}
	err = provider.Health(ctx)
	if err != nil {
		fmt.Printf("Docker is not running and no clickhouse connections details were provided. Skipping IT tests: %s\n", err)
		os.Exit(0)
	}
	fmt.Println("Using Docker for integration tests")
	_, b, _, _ := runtime.Caller(0)
	basePath := filepath.Dir(b)

	expected := []*units.Ulimit{
		{
			Name: "nofile",
			Hard: 262144,
			Soft: 262144,
		},
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, time.Now().UnixNano()); err != nil {
		return ClickHouseTestEnvironment{}, err
	}
	containerName := fmt.Sprintf("clickhouse-go-%x", md5.Sum(buf.Bytes()))

	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("clickhouse/clickhouse-server:%s", GetClickHouseTestVersion()),
		Name:         containerName,
		ExposedPorts: []string{"9000/tcp", "8123/tcp", "9440/tcp", "8443/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("9000/tcp"),
			wait.ForListeningPort("8123/tcp"),
			wait.ForHTTP("/").WithPort("8123/tcp"),
		).WithDeadline(time.Second * 120),
		Mounts: []testcontainers.ContainerMount{
			testcontainers.BindMount(path.Join(basePath, "./resources/custom.xml"), "/etc/clickhouse-server/config.d/custom.xml"),
			testcontainers.BindMount(path.Join(basePath, "./resources/admin.xml"), "/etc/clickhouse-server/users.d/admin.xml"),
			testcontainers.BindMount(path.Join(basePath, "./resources/clickhouse.crt"), "/etc/clickhouse-server/certs/clickhouse.crt"),
			testcontainers.BindMount(path.Join(basePath, "./resources/clickhouse.key"), "/etc/clickhouse-server/certs/clickhouse.key"),
			testcontainers.BindMount(path.Join(basePath, "./resources/CAroot.crt"), "/etc/clickhouse-server/certs/CAroot.crt"),
		},
		Resources: container.Resources{
			Ulimits: expected,
		},
	}

	var clickhouseContainer testcontainers.Container
	for attempt := 0; attempt < 3; attempt++ {
		clickhouseContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err == nil {
			break
		}

		if strings.Contains(err.Error(), "failed to start container") {
			// retry
			continue
		}

		return ClickHouseTestEnvironment{}, err
	}

	p, _ := clickhouseContainer.MappedPort(ctx, "9000")
	hp, _ := clickhouseContainer.MappedPort(ctx, "8123")
	sslPort, _ := clickhouseContainer.MappedPort(ctx, "9440")
	hps, _ := clickhouseContainer.MappedPort(ctx, "8443")
	ip, _ := clickhouseContainer.ContainerIP(ctx)
	testEnv := ClickHouseTestEnvironment{
		ContainerID: clickhouseContainer.GetContainerID(),
		Port:        p.Int(),
		HttpPort:    hp.Int(),
		SslPort:     sslPort.Int(),
		HttpsPort:   hps.Int(),
		Host:        "127.0.0.1",
		Username:    "tester",
		Password:    "ClickHouse",
		Container:   clickhouseContainer,
		ContainerIP: ip,
		Database:    GetEnv("CLICKHOUSE_DATABASE", getDatabaseName(testSet)),
	}
	testEnv.setVersion()

	fmt.Printf("ClickHouse %s ready: Container=%s Host=%s TCP=%d HTTP=%d SSL=%d HTTPS=%d \n",
		testEnv.Version.String(), testEnv.ContainerID[:12], testEnv.Host, testEnv.Port, testEnv.HttpPort, testEnv.SslPort, testEnv.HttpsPort)

	return testEnv, nil
}

func SetTestEnvironment(testSet string, environment ClickHouseTestEnvironment) {
	bytes, err := json.Marshal(environment)
	if err != nil {
		panic(err)
	}
	os.Setenv(fmt.Sprintf("CLICKHOUSE_%s_ENV", strings.ToUpper(testSet)), string(bytes))
}

func GetTestEnvironment(testSet string) (ClickHouseTestEnvironment, error) {
	useDocker, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if err != nil {
		return ClickHouseTestEnvironment{}, err
	}
	if !useDocker {
		return GetExternalTestEnvironment(testSet)
	}
	sEnv := os.Getenv(fmt.Sprintf("CLICKHOUSE_%s_ENV", strings.ToUpper(testSet)))
	if sEnv == "" {
		return ClickHouseTestEnvironment{}, errors.New("unable to find environment")
	}
	var env ClickHouseTestEnvironment
	if err := json.Unmarshal([]byte(sEnv), &env); err != nil {
		return ClickHouseTestEnvironment{}, err
	}
	return env, nil
}

func GetExternalTestEnvironment(testSet string) (ClickHouseTestEnvironment, error) {
	port, err := strconv.Atoi(GetEnv("CLICKHOUSE_PORT", "9000"))
	if err != nil {
		return ClickHouseTestEnvironment{}, nil
	}
	httpPort, err := strconv.Atoi(GetEnv("CLICKHOUSE_HTTP_PORT", "8123"))
	if err != nil {
		return ClickHouseTestEnvironment{}, nil
	}
	sslPort, err := strconv.Atoi(GetEnv("CLICKHOUSE_SSL_PORT", "9440"))
	if err != nil {
		return ClickHouseTestEnvironment{}, nil
	}
	httpsPort, err := strconv.Atoi(GetEnv("CLICKHOUSE_HTTPS_PORT", "8443"))
	if err != nil {
		return ClickHouseTestEnvironment{}, nil
	}
	env := ClickHouseTestEnvironment{
		Port:      port,
		HttpPort:  httpPort,
		SslPort:   sslPort,
		HttpsPort: httpsPort,
		Username:  GetEnv("CLICKHOUSE_USERNAME", "default"),
		Password:  GetEnv("CLICKHOUSE_PASSWORD", ""),
		JWT:       GetEnv("CLICKHOUSE_JWT", ""),
		Host:      GetEnv("CLICKHOUSE_HOST", "localhost"),
		Database:  GetEnv("CLICKHOUSE_DATABASE", getDatabaseName(testSet)),
	}
	env.setVersion()
	return env, nil
}

type NginxReverseHTTPProxyTestEnvironment struct {
	HttpPort       int
	NginxContainer testcontainers.Container `json:"-"`
}

func CreateNginxReverseProxyTestEnvironment(clickhouseEnv ClickHouseTestEnvironment) (NginxReverseHTTPProxyTestEnvironment, error) {
	// create a nginx Container as a reverse proxy
	ctx := context.Background()
	nginxReq := testcontainers.ContainerRequest{
		Image:        "nginx",
		Name:         fmt.Sprintf("nginx-clickhouse-go-%d", time.Now().UnixNano()),
		ExposedPorts: []string{"80/tcp"},
		WaitingFor:   wait.ForListeningPort("80/tcp").WithStartupTimeout(time.Second * time.Duration(120)),
		Cmd:          []string{},
	}
	nginxContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: nginxReq,
		Started:          true,
	})
	if err != nil {
		return NginxReverseHTTPProxyTestEnvironment{}, err
	}
	_, b, _, _ := runtime.Caller(0)
	basePath := filepath.Dir(b)
	nginxConf, err := os.ReadFile(path.Join(basePath, "./resources/nginx.conf"))
	if err != nil {
		return NginxReverseHTTPProxyTestEnvironment{}, err
	}
	// replace upstream clickhouse endpoint
	nginxConf = []byte(strings.Replace(string(nginxConf), "<upstream_http_endpoint>", fmt.Sprintf("%v:8123", clickhouseEnv.ContainerIP), -1))
	err = nginxContainer.CopyToContainer(ctx, nginxConf, "/etc/nginx/nginx.conf", 700)
	if err != nil {
		return NginxReverseHTTPProxyTestEnvironment{}, err
	}
	// reload new nginx.conf and set http proxy upstream
	_, _, err = nginxContainer.Exec(ctx, []string{"nginx", "-s", "reload"})
	if err != nil {
		return NginxReverseHTTPProxyTestEnvironment{}, err
	}
	nginxReloadWaiter := wait.ForHTTP("/clickhouse").WithStartupTimeout(time.Second * time.Duration(120))
	err = nginxReloadWaiter.WaitUntilReady(ctx, nginxContainer)
	if err != nil {
		return NginxReverseHTTPProxyTestEnvironment{}, err
	}
	p, _ := nginxContainer.MappedPort(ctx, "80")
	return NginxReverseHTTPProxyTestEnvironment{
		HttpPort:       p.Int(),
		NginxContainer: nginxContainer,
	}, nil
}

type TinyProxyTestEnvironment struct {
	HttpPort  int
	Container testcontainers.Container `json:"-"`
}

func (e TinyProxyTestEnvironment) ProxyUrl(t *testing.T) string {
	require.NotNil(t, e.Container)

	host, err := e.Container.Host(context.Background())
	require.NoError(t, err)

	return fmt.Sprintf("http://%s:%d", host, e.HttpPort)
}

func CreateTinyProxyTestEnvironment(t *testing.T) (TinyProxyTestEnvironment, error) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "monokal/tinyproxy",
		Name:         fmt.Sprintf("tinyproxy-clickhouse-go-%d", time.Now().UnixNano()),
		ExposedPorts: []string{"8888/tcp"},
		WaitingFor:   wait.ForListeningPort("8888/tcp").WithStartupTimeout(time.Second * time.Duration(120)),
		Cmd:          []string{"--enable-debug", "ANY"},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	p, _ := container.MappedPort(ctx, "8888")
	return TinyProxyTestEnvironment{
		HttpPort:  p.Int(),
		Container: container,
	}, nil
}

func Runtime(m *testing.M, ts string) (exitCode int) {
	ResetRandSeed()
	fmt.Printf("using random seed %d for %s tests\n", randSeed, ts)

	useDocker, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if err != nil {
		panic(err)
	}

	var env ClickHouseTestEnvironment
	switch useDocker {
	case true:
		env, err = CreateClickHouseTestEnvironment(ts)
		if err != nil {
			panic(err)
		}
		defer func() {
			if err := env.Container.Terminate(context.Background()); err != nil {
				panic(err)
			}
		}() //nolint
	case false:
		env, err = GetExternalTestEnvironment(ts)
		if err != nil {
			panic(err)
		}
	}

	SetTestEnvironment(ts, env)
	if err := CreateDatabase(ts); err != nil {
		panic(err)
	}

	return m.Run()
}
