package clickhouse

import (
	"encoding/binary"
	"errors"
	"net"
	"sync/atomic"
	"time"
)

const (
	ClientHelloPacket = 0
)

const (
	ServerHelloPacket     = 0
	ServerDataPacket      = 1
	ServerExceptionPacket = 2
)

var (
	ErrTransactionInProgress   = errors.New("there is already a transaction in progress")
	ErrNoTransactionInProgress = errors.New("there is no transaction in progress")
)

var tick int32

func dial(network string, hosts []string) (*connect, error) {
	var (
		err  error
		conn net.Conn
		abs  = func(v int) int {
			if v < 0 {
				return -1
			}
			return v
		}
		index = abs(int(atomic.AddInt32(&tick, 1)))
	)
	for i := 0; i <= len(hosts); i++ {
		if conn, err = net.DialTimeout(network, hosts[(index+i)%len(hosts)], time.Second); err == nil {
			return &connect{
				Conn: conn,
			}, nil
		}
	}
	return nil, err
}

type connect struct {
	net.Conn
	timezone *time.Location
}

func (conn *connect) writeUInt(i uint) error {
	var (
		buf = make([]byte, binary.MaxVarintLen64)
		len = binary.PutUvarint(buf, uint64(i))
	)
	if _, err := conn.Write(buf[0:len]); err != nil {
		return err
	}
	return nil
}

func (conn *connect) writeString(str string) error {
	if err := conn.writeUInt(uint(len([]byte(str)))); err != nil {
		return err
	}
	if _, err := conn.Write([]byte(str)); err != nil {
		return err
	}
	return nil
}

func (conn *connect) readUInt() (uint, error) {
	v, err := binary.ReadUvarint(conn)
	if err != nil {
		return 0, err
	}
	return uint(v), nil
}

func (conn *connect) ReadByte() (byte, error) {
	b := make([]byte, 1)
	if _, err := conn.Read(b); err != nil {
		return 0x0, err
	}
	return b[0], nil
}

func (conn *connect) readString() (string, error) {
	length, err := conn.readUInt()
	if err != nil {
		return "", err
	}
	str := make([]byte, length)
	if _, err := conn.Read(str); err != nil {
		return "", err
	}
	return string(str), nil
}

func (conn *connect) ping() error {
	return nil
}
