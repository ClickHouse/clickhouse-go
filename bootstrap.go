package clickhouse

import (
	"bufio"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/leakypool"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"github.com/ClickHouse/clickhouse-go/lib/data"
	"github.com/ClickHouse/clickhouse-go/lib/protocol"
)

const (
	// DefaultDatabase when connecting to ClickHouse
	DefaultDatabase = "default"
	// DefaultUsername when connecting to ClickHouse
	DefaultUsername = "default"
	// DefaultConnTimeout when connecting to ClickHouse
	DefaultConnTimeout = 5 * time.Second
	// DefaultReadTimeout when reading query results
	DefaultReadTimeout = time.Minute
	// DefaultWriteTimeout when sending queries
	DefaultWriteTimeout = time.Minute
)

var (
	unixtime    int64
	logOutput   io.Writer = os.Stdout
	hostname, _           = os.Hostname()
	poolInit    sync.Once
)

func init() {
	sql.Register("clickhouse", &bootstrap{})
	go func() {
		for tick := time.Tick(time.Second); ; {
			select {
			case <-tick:
				atomic.AddInt64(&unixtime, int64(time.Second))
			}
		}
	}()
}

func now() time.Time {
	return time.Unix(0, atomic.LoadInt64(&unixtime))
}

type bootstrap struct{}

func (d *bootstrap) Open(dsn string) (driver.Conn, error) {
	return Open(dsn)
}

// SetLogOutput allows to change output of the default logger
func SetLogOutput(output io.Writer) {
	logOutput = output
}

// Open the connection
func Open(dsn string) (driver.Conn, error) {
	clickhouse, err := open(dsn)
	if err != nil {
		return nil, err
	}

	return clickhouse, err
}

func open(dsn string) (*clickhouse, error) {
	dsnParams, err := parseDsn(dsn)
	if err != nil {
		return nil, err
	}

	poolInit.Do(func() {
		leakypool.InitBytePool(dsnParams.poolSize)
	})

	settings, err := makeQuerySettings(dsnParams.queryParams)
	if err != nil {
		return nil, err
	}

	var (
		ch = clickhouse{
			logf:      func(string, ...interface{}) {},
			settings:  settings,
			compress:  dsnParams.compress,
			blockSize: dsnParams.blockSize,
			ServerInfo: data.ServerInfo{
				Timezone: time.Local,
			},
		}
		logger = log.New(logOutput, "[clickhouse]", 0)
	)

	if getBoolFromQuery(dsnParams.queryParams, "debug", false) {
		ch.logf = logger.Printf
	}

	ch.logf("host(s)=%s, database=%s, username=%s",
		strings.Join(dsnParams.hosts, ", "),
		dsnParams.database,
		dsnParams.username,
	)

	options := dsnParams.connOpts

	if ch.conn, err = dial(options); err != nil {
		return nil, err
	}
	logger.SetPrefix(fmt.Sprintf("[clickhouse][connect=%d]", ch.conn.ident))
	ch.buffer = bufio.NewWriter(ch.conn)

	ch.decoder = binary.NewDecoderWithCompress(ch.conn)
	ch.encoder = binary.NewEncoderWithCompress(ch.buffer)

	if err := ch.hello(dsnParams.database, dsnParams.username, dsnParams.password); err != nil {
		ch.conn.Close()
		return nil, err
	}

	return &ch, nil
}

func parseDsn(dsn string) (*dsnQueryParams, error) {
	parsedUrl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	params := parsedUrl.Query()
	hosts := []string{parsedUrl.Host}

	for _, altHost := range strings.Split(params.Get("alt_hosts"), ",") {
		if len(altHost) > 0 {
			hosts = append(hosts, altHost)
		}
	}

	tlsConfigName := getStringFromQuery(params, "tls_config", "")
	tlsConfig := getTLSConfigClone(tlsConfigName)
	if tlsConfigName != "" && tlsConfig == nil {
		return nil, fmt.Errorf("invalid tls_config - no config registered under name %s", tlsConfigName)
	}

	return &dsnQueryParams{
		rawDsn:        dsn,
		parsedUrl:     parsedUrl,
		hosts:         hosts,
		queryParams:   params,
		tlsConfigName: tlsConfigName,

		database: getEscapedStringFromQuery(params, "database", DefaultDatabase),
		username: getEscapedStringFromQuery(params, "username", DefaultUsername),
		password: getEscapedStringFromQuery(params, "password", ""),
		connOpts: connOptions{
			tlsConfig:    tlsConfig,
			openStrategy: getConnOpenStrategyFromQuery(params, connOpenRandom),
			noDelay:      getBoolFromQuery(params, "no_delay", true),
			secure:       getBoolFromQuery(params, "secure", false),
			skipVerify:   getBoolFromQuery(params, "skip_verify", false),
			connTimeout:  getDurationFromQuery(params, "timeout", DefaultConnTimeout),
			readTimeout:  getDurationFromQuery(params, "read_timeout", DefaultReadTimeout),
			writeTimeout: getDurationFromQuery(params, "write_timeout", DefaultWriteTimeout),
		},
		compress:  getBoolFromQuery(params, "compress", false),
		blockSize: getIntFromQuery(params, "block_size", 1000000),
		poolSize:  getIntFromQuery(params, "pool_size", 100),
	}, nil
}

func (ch *clickhouse) hello(database, username, password string) error {
	ch.logf("[hello] -> %s", ch.ClientInfo)
	{
		ch.encoder.Uvarint(protocol.ClientHello)
		if err := ch.ClientInfo.Write(ch.encoder); err != nil {
			return err
		}
		{
			ch.encoder.String(database)
			ch.encoder.String(username)
			ch.encoder.String(password)
		}
		if err := ch.encoder.Flush(); err != nil {
			return err
		}

	}
	{
		packet, err := ch.decoder.Uvarint()
		if err != nil {
			return err
		}
		switch packet {
		case protocol.ServerException:
			return ch.exception()
		case protocol.ServerHello:
			if err := ch.ServerInfo.Read(ch.decoder); err != nil {
				return err
			}
		case protocol.ServerEndOfStream:
			ch.logf("[bootstrap] <- end of stream")
			return nil
		default:
			return fmt.Errorf("[hello] unexpected packet [%d] from server", packet)
		}
	}
	ch.logf("[hello] <- %s", ch.ServerInfo)
	return nil
}
