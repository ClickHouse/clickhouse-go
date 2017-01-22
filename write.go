package clickhouse

import (
	"encoding/binary"
	"io"
)

func writeBool(conn io.Writer, v bool) error {
	value := []byte{0}
	if v {
		value[0] = 1
	}
	if _, err := conn.Write(value); err != nil {
		return err
	}
	return nil
}

func writeUvarint(conn io.Writer, v uint64) error {
	var (
		buf = make([]byte, binary.MaxVarintLen64)
		len = binary.PutUvarint(buf, v)
	)
	if _, err := conn.Write(buf[0:len]); err != nil {
		return err
	}
	return nil
}

func writeString(conn io.Writer, str string) error {
	if err := writeUvarint(conn, uint64(len([]byte(str)))); err != nil {
		return err
	}
	if _, err := conn.Write([]byte(str)); err != nil {
		return err
	}
	return nil
}

func writeInt32(conn *connect, v int32) error {
	if err := binary.Write(conn, binary.LittleEndian, v); err != nil {
		return err
	}
	return nil
}

func writeUInt64(conn *connect, v uint64) error {
	if err := binary.Write(conn, binary.LittleEndian, v); err != nil {
		return err
	}
	return nil
}
