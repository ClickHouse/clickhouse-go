package clickhouse

import (
	"bytes"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultTimeout = time.Minute
)

func init() {
	sql.Register("clickhouse", &connect{})
}

type logger func(format string, v ...interface{})

var (
	ErrTransactionInProgress   = errors.New("there is already a transaction in progress")
	ErrNoTransactionInProgress = errors.New("there is no transaction in progress")
)

var (
	nolog    = func(string, ...interface{}) {}
	debuglog = log.New(os.Stdout, "[clickhouse]", 0).Printf
)

type connect struct {
	http          http.Client
	log           logger
	queries       []string
	buffers       []bytes.Buffer
	inTransaction bool
}

func (conn *connect) Open(dsn string) (driver.Conn, error) {
	url, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	var (
		hosts    = []string{url.Host}
		log      = nolog
		timeout  = DefaultTimeout
		compress bool
	)
	if altHosts := strings.Split(url.Query().Get("alt_hosts"), ","); len(altHosts) != 0 && len(altHosts[0]) != 0 {
		hosts = append(hosts, altHosts...)
	}
	if t, err := strconv.ParseInt(url.Query().Get("timeout"), 10, 64); err == nil && t != 0 {
		timeout = time.Duration(t) * time.Second
	}
	if v, err := strconv.ParseBool(url.Query().Get("compress")); err == nil {
		compress = v
	}
	if debug, err := strconv.ParseBool(url.Query().Get("debug")); err == nil && debug {
		log = debuglog
		log("hosts: %v, timeout: %s, compress: %t", hosts, timeout, compress)
		if username := url.Query().Get("username"); len(username) != 0 {
			log("[basic auth], username: %s, password: %s", username, url.Query().Get("password"))
		}
	}
	return &connect{
		log: log,
		http: http.Client{
			Timeout: timeout,
			Transport: &transport{
				hosts:    hosts,
				scheme:   url.Scheme,
				compress: compress,
				username: url.Query().Get("username"),
				password: url.Query().Get("password"),
				origin: &http.Transport{
					DialContext: (&net.Dialer{
						Timeout:   30 * time.Second,
						KeepAlive: 30 * time.Second,
					}).DialContext,
					MaxIdleConns:          100,
					MaxIdleConnsPerHost:   20,
					IdleConnTimeout:       90 * time.Second,
					TLSHandshakeTimeout:   10 * time.Second,
					ExpectContinueTimeout: 1 * time.Second,
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: true,
					},
				},
			},
		},
	}, nil
}

func (conn *connect) Prepare(query string) (driver.Stmt, error) {
	conn.log("[connect] prepare: %s", query)
	var (
		index    int
		numInput = len(strings.Split(query, "?")) - 1
	)
	if isInsert(query) {
		if conn.inTransaction {
			conn.queries = append(conn.queries, query)
			conn.buffers = append(conn.buffers, bytes.Buffer{})
			index = len(conn.buffers) - 1
			conn.log("[connect] [prepare] tx len: %d", len(conn.queries))
		}
	}
	return &stmt{
		conn:     conn,
		query:    query,
		index:    index,
		numInput: numInput,
	}, nil
}

func (conn *connect) Begin() (driver.Tx, error) {
	conn.log("[connect] begin")
	if conn.inTransaction {
		return nil, ErrTransactionInProgress
	}
	conn.inTransaction = true
	return conn, nil
}

func (conn *connect) Commit() error {
	conn.log("[connect] commit")
	defer conn.reset()
	if !conn.inTransaction {
		return ErrNoTransactionInProgress
	}
	for index, query := range conn.queries {
		if _, err := conn.do(query, &conn.buffers[index]); err != nil {
			return err
		}
	}
	return nil
}

func (conn *connect) do(query string, data *bytes.Buffer /*io.Reader*/) (io.Reader, error) {
	query = formatQuery(query)
	conn.log("[connect] [do] format query: %s", query)
	response, err := conn.http.Post("?"+(&url.Values{"query": []string{query}}).Encode(), "application/x-www-form-urlencoded", data)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		message, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(string(message))
	}
	var body bytes.Buffer
	if _, err := io.Copy(&body, response.Body); err != nil {
		return nil, err
	}
	return &body, nil
}

func (conn *connect) Rollback() error {
	conn.log("[connect] rollback")
	defer conn.reset()
	if !conn.inTransaction {
		return ErrNoTransactionInProgress
	}
	return nil
}

func (conn *connect) Close() error {
	conn.log("[connect] close")
	conn.reset()
	return nil
}

func (conn *connect) reset() {
	conn.log("[connect] reset")
	if conn.inTransaction {
		conn.inTransaction = false
		for _, buffer := range conn.buffers {
			buffer.Reset()
		}
		conn.queries = conn.queries[0:0]
		conn.buffers = conn.buffers[0:0]
	}
}
