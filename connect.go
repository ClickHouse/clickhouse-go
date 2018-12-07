package clickhouse

import (
	"bufio"
	"crypto/tls"
	"database/sql/driver"
	"net"
	"sync/atomic"
	"time"
)

var tick int32

type openStrategy int8

func (s openStrategy) String() string {
	switch s {
	case connOpenInOrder:
		return "in_order"
	}
	return "random"
}

const (
	connOpenRandom openStrategy = iota + 1
	connOpenInOrder
)

func dial(secure, skipVerify bool, hosts []string, readTimeout, writeTimeout time.Duration, noDelay bool, openStrategy openStrategy, logf func(string, ...interface{})) (*connect, error) {
	var (
		err error
		abs = func(v int) int {
			if v < 0 {
				return -1 * v
			}
			return v
		}
		conn  net.Conn
		ident = abs(int(atomic.AddInt32(&tick, 1)))
	)
	for i := range hosts {
		var num int
		switch openStrategy {
		case connOpenInOrder:
			num = i
		case connOpenRandom:
			num = (ident + i) % len(hosts)
		}
		switch {
		case secure:
			conn, err = tls.DialWithDialer(
				&net.Dialer{
					Timeout: 5 * time.Second,
				},
				"tcp",
				hosts[num],
				&tls.Config{
					InsecureSkipVerify: skipVerify,
				})
		default:
			conn, err = net.DialTimeout("tcp", hosts[num], 5*time.Second)
		}
		if err == nil {
			logf("[dial] secure=%t, skip_verify=%t, strategy=%s, ident=%d, server=%d -> %s", secure, skipVerify, openStrategy, ident, num, conn.RemoteAddr())
			if tcp, ok := conn.(*net.TCPConn); ok {
				tcp.SetNoDelay(noDelay) // Disable or enable the Nagle Algorithm for this tcp socket
			}
			return &connect{
				Conn:         conn,
				logf:         logf,
				ident:        ident,
				buffer:       bufio.NewReader(conn),
				readTimeout:  readTimeout,
				writeTimeout: writeTimeout,
			}, nil
		}
	}
	return nil, err
}

type connect struct {
	net.Conn
	logf                  func(string, ...interface{})
	ident                 int
	buffer                *bufio.Reader
	closed                bool
	readTimeout           time.Duration
	writeTimeout          time.Duration
	lastReadDeadlineTime  time.Time
	lastWriteDeadlineTime time.Time
}

func (conn *connect) Read(b []byte) (int, error) {
	var (
		n      int
		err    error
		total  int
		dstLen = len(b)
	)
	if currentTime := now(); conn.readTimeout != 0 && currentTime.Sub(conn.lastReadDeadlineTime) > (conn.readTimeout>>2) {
		conn.SetReadDeadline(time.Now().Add(conn.readTimeout))
		conn.lastReadDeadlineTime = currentTime
	}
	for total < dstLen {
		if n, err = conn.buffer.Read(b[total:]); err != nil {
			conn.logf("[connect] read error: %v", err)
			conn.Close()
			return n, driver.ErrBadConn
		}
		total += n
	}
	return total, nil
}

func (conn *connect) Write(b []byte) (int, error) {
	var (
		n      int
		err    error
		total  int
		srcLen = len(b)
	)
	if currentTime := now(); conn.writeTimeout != 0 && currentTime.Sub(conn.lastWriteDeadlineTime) > (conn.writeTimeout>>2) {
		conn.SetWriteDeadline(time.Now().Add(conn.writeTimeout))
		conn.lastWriteDeadlineTime = currentTime
	}
	for total < srcLen {
		if n, err = conn.Conn.Write(b[total:]); err != nil {
			conn.logf("[connect] write error: %v", err)
			conn.Close()
			return n, driver.ErrBadConn
		}
		total += n
	}
	return n, nil
}

func (conn *connect) Close() error {
	if !conn.closed {
		conn.closed = true
		return conn.Conn.Close()
	}
	return nil
}
