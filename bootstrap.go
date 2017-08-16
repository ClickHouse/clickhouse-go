package clickhouse

import (
	"bufio"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

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

var hostname, _ = os.Hostname()

func init() {
	sql.Register("clickhouse", &bootstrap{})
}

type bootstrap struct{}

func (d *bootstrap) Open(dsn string) (driver.Conn, error) {
	return Open(dsn)
}

func Open(dsn string) (driver.Conn, error) {
	url, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	var (
		hosts        = []string{url.Host}
		noDelay      = true
		compress     = false
		database     = url.Query().Get("database")
		username     = url.Query().Get("username")
		password     = url.Query().Get("password")
		readTimeout  = DefaultReadTimeout
		writeTimeout = DefaultWriteTimeout
		blockSize    = 1000000
	)
	if len(database) == 0 {
		database = DefaultDatabase
	}
	if len(username) == 0 {
		username = DefaultUsername
	}
	if v, err := strconv.ParseBool(url.Query().Get("no_delay")); err == nil && !v {
		noDelay = false
	}
	if duration, err := strconv.ParseInt(url.Query().Get("read_timeout"), 10, 64); err == nil {
		readTimeout = time.Duration(duration) * time.Second
	}
	if duration, err := strconv.ParseInt(url.Query().Get("write_timeout"), 10, 64); err == nil {
		writeTimeout = time.Duration(duration) * time.Second
	}
	if size, err := strconv.ParseInt(url.Query().Get("block_size"), 10, 64); err == nil {
		blockSize = int(size)
	}
	if altHosts := strings.Split(url.Query().Get("alt_hosts"), ","); len(altHosts) != 0 {
		for _, host := range altHosts {
			if len(host) != 0 {
				hosts = append(hosts, host)
			}
		}
	}
	if v, err := strconv.ParseBool(url.Query().Get("compress")); err == nil && v {
		//compress = true
	}

	ch := clickhouse{
		logf:      func(string, ...interface{}) {},
		compress:  compress,
		blockSize: blockSize,
		ServerInfo: data.ServerInfo{
			Timezone: time.Local,
		},
	}
	if debug, err := strconv.ParseBool(url.Query().Get("debug")); err == nil && debug {
		ch.logf = log.New(os.Stdout, "[clickhouse]", 0).Printf
	}
	ch.logf("host(s)=%s, database=%s, username=%s",
		strings.Join(hosts, ", "),
		database,
		username,
	)
	if ch.conn, err = dial("tcp", hosts, noDelay, readTimeout, writeTimeout, ch.logf); err != nil {
		return nil, err
	}
	ch.buffer = bufio.NewReadWriter(
		bufio.NewReader(ch.conn),
		bufio.NewWriter(ch.conn),
	)
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
		if err := ch.buffer.Flush(); err != nil {
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
			return ch.exception(ch.decoder)
		case protocol.ServerHello:
			if err := ch.ServerInfo.Read(ch.decoder); err != nil {
				return err
			}
		default:
			ch.conn.Close()
			return fmt.Errorf("unexpected packet [%d] from server", packet)
		}
	}
	ch.logf("[hello] <- %s", ch.ServerInfo)
	return nil
}
