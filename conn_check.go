//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos
// +build linux darwin dragonfly freebsd netbsd openbsd solaris illumos

package clickhouse

import (
	"crypto/tls"
	"io"
	"syscall"
)

// connCheck probes the raw socket of an idle connection without consuming any
// data (MSG_PEEK). A healthy idle connection has an empty receive buffer: the
// ClickHouse native protocol has no server-initiated packets between queries,
// so pending bytes mean the previous response was not fully drained, and a
// zero-length read means the server closed the connection.
func (c *connect) connCheck() error {
	conn := c.conn
	if tlsConn, ok := c.conn.(*tls.Conn); ok {
		conn = tlsConn.NetConn()
	}

	var sysErr error
	sysConn, ok := conn.(syscall.Conn)
	if !ok {
		return nil
	}
	rawConn, err := sysConn.SyscallConn()
	if err != nil {
		return err
	}

	err = rawConn.Read(func(fd uintptr) bool {
		var buf [1]byte
		// The runtime keeps the fd non-blocking, so an empty receive buffer
		// returns EAGAIN rather than blocking.
		n, _, err := syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK)
		switch {
		case n == 0 && err == nil:
			sysErr = io.EOF
		case n > 0:
			sysErr = errUnexpectedRead
		case err == syscall.EAGAIN || err == syscall.EWOULDBLOCK:
			sysErr = nil
		default:
			sysErr = err
		}
		return true
	})
	if err != nil {
		return err
	}

	return sysErr
}
