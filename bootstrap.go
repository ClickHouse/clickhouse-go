package clickhouse

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultDatabase     = "default"
	DefaultUsername     = "default"
	DefaultReadTimeout  = 30 * time.Second
	DefaultWriteTimeout = time.Minute
)

const ClientName = "Golang SQLDriver"
const (
	ClickHouseRevision         = 54213
	ClickHouseDBMSVersionMajor = 1
	ClickHouseDBMSVersionMinor = 1
)

const (
	DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES         = 50264
	DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS   = 51554
	DBMS_MIN_REVISION_WITH_BLOCK_INFO               = 51903
	DBMS_MIN_REVISION_WITH_CLIENT_INFO              = 54032
	DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE          = 54058
	DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060
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
		database     = url.Query().Get("database")
		username     = url.Query().Get("username")
		password     = url.Query().Get("password")
		readTimeout  = DefaultReadTimeout
		writeTimeout = DefaultWriteTimeout
		blockSize    = 100000
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
	ch := clickhouse{
		logf:           func(string, ...interface{}) {},
		blockSize:      blockSize,
		serverTimezone: time.Local,
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
	if err := ch.hello(database, username, password); err != nil {
		return nil, err
	}
	return &ch, nil
}

func (ch *clickhouse) hello(database, username, password string) error {
	ch.logf("[hello] -> %s %d.%d.%d",
		ClientName,
		ClickHouseDBMSVersionMajor,
		ClickHouseDBMSVersionMinor,
		ClickHouseRevision,
	)
	{
		writeUvarint(ch.conn, ClientHelloPacket)
		writeString(ch.conn, ClientName)
		writeUvarint(ch.conn, ClickHouseDBMSVersionMajor)
		writeUvarint(ch.conn, ClickHouseDBMSVersionMinor)
		writeUvarint(ch.conn, ClickHouseRevision)
		writeString(ch.conn, database)
		writeString(ch.conn, username)
		writeString(ch.conn, password)
	}
	{
		packet, err := readUvarint(ch.conn)
		if err != nil {
			return err
		}
		switch packet {
		case ServerExceptionPacket:
			return ch.exception()
		case ServerHelloPacket:
			var err error
			if ch.serverName, err = readString(ch.conn); err != nil {
				return err
			}
			if ch.serverVersionMinor, err = readUvarint(ch.conn); err != nil {
				return err
			}
			if ch.serverVersionMajor, err = readUvarint(ch.conn); err != nil {
				return err
			}
			if ch.serverRevision, err = readUvarint(ch.conn); err != nil {
				return err
			}
			if ch.serverRevision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
				timezone, err := readString(ch.conn)
				if err != nil {
					return err
				}
				if ch.serverTimezone, err = time.LoadLocation(timezone); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected packet [%d] from server", packet)
		}
	}
	ch.logf("[hello] <- %s %d.%d.%d (%s)",
		ch.serverName,
		ch.serverVersionMajor,
		ch.serverVersionMinor,
		ch.serverRevision,
		ch.serverTimezone,
	)
	return nil
}
