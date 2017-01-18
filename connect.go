package clickhouse

import (
	"bytes"
	"encoding/binary"
	"net"
	"sync/atomic"
	"time"
)

var tick int32

func dial(network string, hosts []string, timeout time.Duration) (*connect, error) {
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
		if conn, err = net.DialTimeout(network, hosts[(index+i)%len(hosts)], timeout); err == nil {
			if tcp, ok := conn.(*net.TCPConn); ok {
				tcp.SetNoDelay(true)
			}
			return &connect{
				Conn: conn,
			}, nil
		}
	}
	return nil, err
}

type connect struct {
	net.Conn
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

func (conn *connect) readFixed(len int) ([]byte, error) {
	buf := make([]byte, len)
	if _, err := conn.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (conn *connect) readString() (string, error) {
	len, err := conn.readUInt()
	if err != nil {
		return "", err
	}
	str, err := conn.readFixed(int(len))
	if err != nil {
		return "", err
	}
	return string(str), nil
}

func (conn *connect) readBinaryBool() (bool, error) {
	bytes, err := conn.readFixed(1)
	if err != nil {
		return false, err
	}
	return bytes[0] == 1, nil
}

func (conn *connect) readBinaryInt32() (int32, error) {
	var v int32
	buf, err := conn.readFixed(4)
	if err != nil {
		return 0, err
	}
	if err := binary.Read(bytes.NewBuffer(buf), binary.LittleEndian, &v); err != nil {
		return 0, err
	}
	return v, nil
}

func (conn *connect) ReadByte() (byte, error) {
	bytes, err := conn.readFixed(1)
	if err != nil {
		return 0x0, err
	}
	return bytes[0], nil
}
