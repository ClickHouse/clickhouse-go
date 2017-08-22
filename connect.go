package clickhouse

import (
	"bufio"
	"database/sql/driver"
	"net"
	"sync/atomic"
	"time"
)

var tick int32

func dial(network string, hosts []string, noDelay bool, r, w time.Duration, logf func(string, ...interface{})) (*connect, error) {
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
	for i := 0; i <= len(hosts); i++ {
		num := (ident + 1) % len(hosts)
		if conn, err = net.DialTimeout(network, hosts[num], 20*time.Second); err == nil {
			logf("[connect=%d] server=%d -> %s", ident, num, conn.RemoteAddr())
			if tcp, ok := conn.(*net.TCPConn); ok {
				tcp.SetNoDelay(noDelay) // Disable or enable the Nagle Algorithm for this tcp socket
			}
			return &connect{
				Conn:         conn,
				logf:         logf,
				ident:        ident,
				buffer:       bufio.NewReaderSize(conn, 4096*256),
				readTimeout:  r,
				writeTimeout: w,
			}, nil
		}
	}
	return nil, err
}

type connect struct {
	net.Conn
	logf         func(string, ...interface{})
	ident        int
	buffer       *bufio.Reader
	closed       bool
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (conn *connect) Read(b []byte) (int, error) {
	if conn.readTimeout != 0 {
		conn.SetReadDeadline(time.Now().Add(conn.readTimeout))
	}
	var (
		n      int
		err    error
		total  int
		dstLen = len(b)
	)
	for total < dstLen {
		if n, err = conn.buffer.Read(b[total:]); err != nil {
			conn.logf("[connect] read error: %v", err)
			conn.closed = true
			return n, driver.ErrBadConn
		}
		total += n
	}
	return total, nil
}

func (conn *connect) Write(b []byte) (int, error) {
	if conn.writeTimeout != 0 {
		conn.SetWriteDeadline(time.Now().Add(conn.writeTimeout))
	}
	var (
		n      int
		err    error
		total  int
		srcLen = len(b)
	)
	for total < srcLen {
		if n, err = conn.Conn.Write(b[total:]); err != nil {
			conn.logf("[connect] write error: %v", err)
			conn.closed = true
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
