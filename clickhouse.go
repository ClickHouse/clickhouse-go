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

var (
	ErrInsertInNotBatchMode    = errors.New("insert statement supported only in the batch mode (use begin/commit)")
	ErrLimitBatchStatementInTx = errors.New("other batch request has already been prepared in transaction")
)

func (ch *clickhouse) Prepare(query string) (driver.Stmt, error) {
	ch.log("[prepare] %s", query)
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

func (ch *clickhouse) insert(query string) (driver.Stmt, error) {
	if err := ch.sendQuery(formatQuery(query)); err != nil {
		return nil, err
	}
	datapacket, err := ch.datapacket()
	if err != nil {
		return nil, err
	}
	if ch.batch != nil {
		return nil, ErrLimitBatchStatementInTx
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

func (ch *clickhouse) Begin() (driver.Tx, error) {
	if ch.inTransaction {
		return nil, sql.ErrTxDone
	}
	ch.batch = nil
	ch.inTransaction = true
	return ch, nil
}

func (ch *clickhouse) Rollback() error {
	if !ch.inTransaction {
		return sql.ErrTxDone
	}
	ch.batch = nil
	ch.inTransaction = false
	return nil
}

func (ch *clickhouse) Commit() error {
	if !ch.inTransaction {
		return sql.ErrTxDone
	}
	ch.batch = nil
	ch.inTransaction = false
	//send batch request
	/*
		stmt.ch.conn.writeUInt(ClientDataPacket)
		stmt.ch.conn.writeString("") //tmp
		stmt.datapacket.blockInfo.write(stmt.ch.conn)
		stmt.ch.conn.writeUInt(stmt.datapacket.numColumns)
		stmt.ch.conn.writeUInt(2)

		for _, name := range []string{"os_id", "browser_id"} {
			fmt.Println("Write", name)
			stmt.ch.conn.writeString(name)
			stmt.ch.conn.writeString("UInt8")
			fmt.Println(binary.Write(stmt.ch.conn, binary.LittleEndian, uint8(44)))
			fmt.Println(binary.Write(stmt.ch.conn, binary.LittleEndian, uint8(88)))
		}
		fmt.Println("DONE", stmt.ch.ping())
		fmt.Println(stmt.ch.receivePacket())
	*/
	if err := ch.ping(); err != nil {
		return err
	}
	if _, err := ch.receivePacket(); err != nil {
		return err
	}
	return nil
}

func (ch *clickhouse) Close() error {
	return ch.conn.Close()
}
