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
	ClientName = "Golang SQLDriver"
)

const (
	ClickHouseRevision         = 54126
	ClickHouseDBMSVersionMajor = 1
	ClickHouseDBMSVersionMinor = 1
)

const (
	DefaultDatabase = "default"
	DefaultUsername = "default"
)

type logger func(format string, v ...interface{})

var (
	nolog       = func(string, ...interface{}) {}
	debuglog    = log.New(os.Stdout, "[clickhouse]", 0).Printf
	hostname, _ = os.Hostname()
)

func init() {
	sql.Register("clickhouse", &clickhouse{})
}

type clickhouse struct {
	log                logger
	conn               *connect
	compress           bool
	serverName         string
	serverRevision     uint
	serverVersionMinor uint
	serverVersionMajor uint
	serverTimezone     *time.Location
}

func (ch *clickhouse) Open(dsn string) (driver.Conn, error) {
	url, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	var (
		hosts    = []string{url.Host}
		database = url.Query().Get("database")
		username = url.Query().Get("username")
		password = url.Query().Get("password")
	)
	if len(database) == 0 {
		database = DefaultDatabase
	}
	if len(username) == 0 {
		username = DefaultUsername
	}
	ch.log = nolog
	ch.serverTimezone = time.UTC
	if debug, err := strconv.ParseBool(url.Query().Get("debug")); err == nil && debug {
		ch.log = debuglog
	}
	if compress, err := strconv.ParseBool(url.Query().Get("compress")); err == nil {
		ch.compress = compress
	}
	if altHosts := strings.Split(url.Query().Get("alt_hosts"), ","); len(altHosts) != 0 {
		for _, host := range altHosts {
			if len(host) != 0 {
				hosts = append(hosts, host)
			}
		}
	}
	ch.log("host(s): %s, database: %s, username: %s, compress: %t",
		strings.Join(hosts, ", "),
		database,
		username,
		ch.compress,
	)
	if ch.conn, err = dial(url.Scheme, hosts); err != nil {
		return nil, err
	}
	if err := ch.hello(database, username, password); err != nil {
		return nil, err
	}
	return ch, nil
}

func (ch *clickhouse) Prepare(query string) (driver.Stmt, error) {
	ch.log("[prepare] %s", query)
	if isInsert(query) {
		return ch.insert(query)
	}
	return &stmt{
		ch:       ch,
		query:    query,
		numInput: strings.Count(query, "?"),
	}, nil
}

func (ch *clickhouse) insert(query string) (driver.Stmt, error) {
	if err := ch.sendQuery(formatQuery(query)); err != nil {
		return nil, err
	}
	datapacket, err := ch.datapacket()
	if err != nil {
		return nil, err
	}
	return &stmt{
		ch:           ch,
		isInsert:     true,
		numInput:     strings.Count(query, "?"),
		columnsTypes: datapacket.columnsTypes,
		datapacket:   datapacket,
	}, nil
}

func (ch *clickhouse) sendQuery(query string) error {
	ch.log("[send query] %s", query)
	if err := ch.conn.writeUInt(ClientQueryPacket); err != nil {
		return err
	}
	if err := ch.conn.writeString(""); err != nil { // queryID
		return err
	}
	if ch.serverRevision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO {
		ch.conn.writeUInt(1)
		ch.conn.writeString("")
		ch.conn.writeString("") //initial_query_id
		ch.conn.writeString("[::ffff:127.0.0.1]:0")
		ch.conn.writeUInt(1) // iface type TCP
		ch.conn.writeString(hostname)
		ch.conn.writeString("localhost")
		ch.conn.writeString(ClientName)
		ch.conn.writeUInt(ClickHouseDBMSVersionMajor)
		ch.conn.writeUInt(ClickHouseDBMSVersionMinor)
		ch.conn.writeUInt(ClickHouseRevision)
		if ch.serverRevision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
			ch.conn.writeString("")
		}
	}
	if err := ch.conn.writeString(""); err != nil { // settings
		return err
	}
	if err := ch.conn.writeUInt(StateComplete); err != nil {
		return err
	}
	if err := ch.conn.writeUInt(0); err != nil { // compress
		return err
	}
	if err := ch.conn.writeString(query); err != nil {
		return err
	}
	{ // datablock
		if err := ch.conn.writeUInt(ClientDataPacket); err != nil {
			return err
		}
		if ch.serverRevision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
			if err := ch.conn.writeString(""); err != nil {
				return err
			}
		}
		for _, z := range []uint{0, 0, 0} { // empty block
			if err := ch.conn.writeUInt(z); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ch *clickhouse) Begin() (driver.Tx, error) {
	return ch, nil
}

func (ch *clickhouse) Rollback() error {
	return nil
}

func (ch *clickhouse) Commit() error {
	return nil
}

func (ch *clickhouse) Close() error {
	return ch.conn.Close()
}

func (ch *clickhouse) hello(database, username, password string) error {
	ch.log("[hello] -> %s %d.%d.%d",
		ClientName,
		ClickHouseDBMSVersionMajor,
		ClickHouseDBMSVersionMinor,
		ClickHouseRevision,
	)
	{
		ch.conn.writeUInt(ClientHelloPacket)
		ch.conn.writeString(ClientName)
		ch.conn.writeUInt(ClickHouseDBMSVersionMajor)
		ch.conn.writeUInt(ClickHouseDBMSVersionMinor)
		ch.conn.writeUInt(ClickHouseRevision)
		ch.conn.writeString(database)
		ch.conn.writeString(username)
		ch.conn.writeString(password)
	}
	{
		packet, err := ch.conn.readUInt()
		if err != nil {
			return err
		}
		switch packet {
		case ServerExceptionPacket:
			return ch.exception()
		case ServerHelloPacket:
			var err error
			if ch.serverName, err = ch.conn.readString(); err != nil {
				return err
			}
			if ch.serverVersionMinor, err = ch.conn.readUInt(); err != nil {
				return err
			}
			if ch.serverVersionMajor, err = ch.conn.readUInt(); err != nil {
				return err
			}
			if ch.serverRevision, err = ch.conn.readUInt(); err != nil {
				return err
			}
			if ch.serverRevision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
				timezone, err := ch.conn.readString()
				if err != nil {
					return err
				}
				if ch.serverTimezone, err = time.LoadLocation(timezone); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("Unexpected packet from server")
		}
	}
	ch.log("[hello] <- %s %d.%d.%d (%s)",
		ch.serverName,
		ch.serverVersionMajor,
		ch.serverVersionMinor,
		ch.serverRevision,
		ch.serverTimezone,
	)
	return nil
}

func (ch *clickhouse) ping() error {
	ch.log("-> ping")
	if err := ch.conn.writeUInt(ClientPingPacket); err != nil {
		return err
	}
	packet, err := ch.conn.readUInt()
	if err != nil {
		return err
	}
	for packet == ServerProgressPacket {
		if _, err = ch.progress(); err != nil {
			return err
		}
		if packet, err = ch.conn.readUInt(); err != nil {
			return err
		}
	}
	if packet != ServerPongPacket {
		return fmt.Errorf("Unexpected packet from server")
	}
	ch.log("<- pong")
	return nil
}
