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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kshvakov/clickhouse/lib/leakypool"

	"github.com/kshvakov/clickhouse/lib/binary"
	"github.com/kshvakov/clickhouse/lib/data"
	"github.com/kshvakov/clickhouse/lib/protocol"
)

const (
	DefaultDatabase     = "default"
	DefaultUsername     = "default"
	DefaultReadTimeout  = time.Minute
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
	return time.Unix(atomic.LoadInt64(&unixtime), 0)
}

type bootstrap struct{}

func (d *bootstrap) Open(dsn string) (driver.Conn, error) {
	return Open(dsn)
}

func SetLogOutput(output io.Writer) {
	logOutput = output
}

func Open(dsn string) (driver.Conn, error) {
	return open(dsn)
}

func open(dsn string) (*clickhouse, error) {
	url, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	var (
		hosts            = []string{url.Host}
		query            = url.Query()
		secure           = false
		skipVerify       = true
		noDelay          = true
		compress         = false
		database         = query.Get("database")
		username         = query.Get("username")
		password         = query.Get("password")
		blockSize        = 1000000
		readTimeout      = DefaultReadTimeout
		writeTimeout     = DefaultWriteTimeout
		connOpenStrategy = connOpenRandom
		poolSize         = 100
	)
	if len(database) == 0 {
		database = DefaultDatabase
	}
	if len(username) == 0 {
		username = DefaultUsername
	}
	if v, err := strconv.ParseBool(query.Get("no_delay")); err == nil && !v {
		noDelay = false
	}
	if v, err := strconv.ParseBool(query.Get("secure")); err == nil && v {
		secure = true
	}
	if v, err := strconv.ParseBool(query.Get("skip_verify")); err == nil && !v {
		skipVerify = false
	}
	if duration, err := strconv.ParseFloat(query.Get("read_timeout"), 64); err == nil {
		readTimeout = time.Duration(duration * float64(time.Second))
	}
	if duration, err := strconv.ParseFloat(query.Get("write_timeout"), 64); err == nil {
		writeTimeout = time.Duration(duration * float64(time.Second))
	}
	if size, err := strconv.ParseInt(query.Get("block_size"), 10, 64); err == nil {
		blockSize = int(size)
	}
	if size, err := strconv.ParseInt(query.Get("pool_size"), 10, 64); err == nil {
		poolSize = int(size)
	}
	poolInit.Do(func() {
		leakypool.InitBytePool(poolSize)
	})
	if altHosts := strings.Split(query.Get("alt_hosts"), ","); len(altHosts) != 0 {
		for _, host := range altHosts {
			if len(host) != 0 {
				hosts = append(hosts, host)
			}
		}
	}
	switch query.Get("connection_open_strategy") {
	case "random":
		connOpenStrategy = connOpenRandom
	case "in_order":
		connOpenStrategy = connOpenInOrder
	}

	if v, err := strconv.ParseBool(query.Get("compress")); err == nil && v {
		compress = true
	}

	var (
		ch = clickhouse{
			logf:      func(string, ...interface{}) {},
			compress:  compress,
			blockSize: blockSize,
			ServerInfo: data.ServerInfo{
				Timezone: time.Local,
			},
		}
		logger = log.New(logOutput, "[clickhouse]", 0)
	)
	if debug, err := strconv.ParseBool(url.Query().Get("debug")); err == nil && debug {
		ch.logf = logger.Printf
	}
	ch.logf("host(s)=%s, database=%s, username=%s",
		strings.Join(hosts, ", "),
		database,
		username,
	)
	if ch.conn, err = dial(secure, skipVerify, hosts, readTimeout, writeTimeout, noDelay, connOpenStrategy, ch.logf); err != nil {
		return nil, err
	}
	logger.SetPrefix(fmt.Sprintf("[clickhouse][connect=%d]", ch.conn.ident))
	ch.buffer = bufio.NewWriter(ch.conn)

	ch.decoder = binary.NewDecoder(ch.conn)
	ch.encoder = binary.NewEncoder(ch.buffer)

	if err := ch.hello(database, username, password); err != nil {
		return nil, err
	}
	return &ch, nil
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
			ch.conn.Close()
			return fmt.Errorf("[hello] unexpected packet [%d] from server", packet)
		}
	}
	ch.logf("[hello] <- %s", ch.ServerInfo)
	return nil
}
