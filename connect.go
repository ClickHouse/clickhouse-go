package clickhouse

import (
	"net"
	"sync/atomic"
	"time"
)

var tick int32

func dial(network string, hosts []string, r, w time.Duration) (*connect, error) {
	var (
		err error
		abs = func(v int) int {
			if v < 0 {
				return -1
			}
			return v
		}
		conn  net.Conn
		index = abs(int(atomic.AddInt32(&tick, 1)))
	)
	for i := 0; i <= len(hosts); i++ {
		if conn, err = net.DialTimeout(network, hosts[(index+i)%len(hosts)], 2*time.Second); err == nil {
			if tcp, ok := conn.(*net.TCPConn); ok {
				tcp.SetNoDelay(true)
			}
			return &connect{
				Conn:         conn,
				timeLocation: time.Local,
				readTimeout:  r,
				writeTimeout: w,
			}, nil
		}
	}
	return nil, err
}

type connect struct {
	net.Conn
	timeLocation *time.Location
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (conn *connect) Read(b []byte) (int, error) {
	if conn.readTimeout != 0 {
		conn.SetReadDeadline(time.Now().Add(conn.readTimeout))
	}
	return conn.Conn.Read(b)
}

func (conn *connect) Write(b []byte) (int, error) {
	if conn.writeTimeout != 0 {
		conn.SetWriteDeadline(time.Now().Add(conn.writeTimeout))
	}
	return conn.Conn.Write(b)
}

func (conn *connect) ReadByte() (byte, error) {
	bytes, err := readFixed(conn, 1)
	if err != nil {
		return 0x0, err
	}
	return bytes[0], nil
}
