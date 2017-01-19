package clickhouse

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
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
	inTransaction      bool
	batch              *batch
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
		timeout  = time.Second
	)
	if len(database) == 0 {
		database = DefaultDatabase
	}
	if len(username) == 0 {
		username = DefaultUsername
	}
	ch = &clickhouse{
		log:            nolog,
		serverTimezone: time.UTC,
	}
	if debug, err := strconv.ParseBool(url.Query().Get("debug")); err == nil && debug {
		ch.log = debuglog
	}
	if compress, err := strconv.ParseBool(url.Query().Get("compress")); err == nil {
		ch.compress = compress
	}
	if t, err := strconv.ParseInt(url.Query().Get("timeout"), 10, 64); err == nil {
		timeout = time.Duration(t) * time.Second
	}
	if altHosts := strings.Split(url.Query().Get("alt_hosts"), ","); len(altHosts) != 0 {
		for _, host := range altHosts {
			if len(host) != 0 {
				hosts = append(hosts, host)
			}
		}
	}
	ch.log("host(s)=%s, database=%s, username=%s, compress=%t",
		strings.Join(hosts, ", "),
		database,
		username,
		ch.compress,
	)
	if ch.conn, err = dial(url.Scheme, hosts, timeout); err != nil {
		return nil, err
	}
	if err := ch.hello(database, username, password); err != nil {
		return nil, err
	}
	return ch, nil
}

var (
	ErrInsertInNotBatchMode    = errors.New("insert statement supported only in the batch mode (use begin/commit)")
	ErrLimitBatchStatementInTx = errors.New("batch request has already been prepared in transaction")
)

func (ch *clickhouse) Prepare(query string) (driver.Stmt, error) {
	ch.log("[prepare] %s", query)
	if ch.batch != nil {
		return nil, ErrLimitBatchStatementInTx
	}
	if isInsert(query) {
		if !ch.inTransaction {
			return nil, ErrInsertInNotBatchMode
		}
		return ch.insert(query)
	}
	return &stmt{
		ch:       ch,
		query:    query,
		numInput: strings.Count(query, "?"),
	}, nil
}

func (ch *clickhouse) Begin() (driver.Tx, error) {
	ch.log("[begin] tx=%t, batch=%t", ch.inTransaction, ch.batch != nil)
	if ch.inTransaction {
		return nil, sql.ErrTxDone
	}
	ch.batch = nil
	ch.inTransaction = true
	return ch, nil
}

func (ch *clickhouse) Rollback() error {
	ch.log("[rollback] tx=%t, batch=%t", ch.inTransaction, ch.batch != nil)
	if !ch.inTransaction {
		return sql.ErrTxDone
	}
	ch.batch = nil
	ch.inTransaction = false
	ch.conn.writeUInt(ClientCancelPacket)
	if err := ch.ping(); err != nil {
		return err
	}
	return nil
}

func (ch *clickhouse) Commit() error {
	ch.log("[commit] tx=%t, batch=%t", ch.inTransaction, ch.batch != nil)
	if !ch.inTransaction {
		return sql.ErrTxDone
	}
	defer func() {
		ch.batch = nil
		ch.inTransaction = false
	}()
	if ch.batch != nil {
		if err := ch.batch.sendData(ch.conn); err != nil {
			return err
		}
		if err := ch.ping(); err != nil {
			return err
		}
		if _, err := ch.receivePacket(); err != nil {
			return err
		}
	}
	return nil
}

func (ch *clickhouse) Close() error {
	ch.log("[close]")
	return ch.conn.Close()
}

func (ch *clickhouse) insert(query string) (driver.Stmt, error) {
	if err := ch.sendQuery(formatQuery(query)); err != nil {
		return nil, err
	}
	datapacket, err := ch.datapacket()
	if err != nil {
		return nil, err
	}
	ch.batch = &batch{
		datapacket: datapacket,
	}
	return &stmt{
		ch:       ch,
		isInsert: true,
		numInput: strings.Count(query, "?"),
	}, nil
}
