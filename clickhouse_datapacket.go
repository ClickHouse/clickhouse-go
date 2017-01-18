package clickhouse

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
)

type blockInfo struct {
	num1        uint
	isOverflows bool
	num2        uint
	bucketNum   int32
	num3        uint
}

func (info *blockInfo) read(conn *connect) error {
	var err error
	if info.num1, err = conn.readUInt(); err != nil {
		return err
	}
	if info.isOverflows, err = conn.readBinaryBool(); err != nil {
		return err
	}
	if info.num2, err = conn.readUInt(); err != nil {
		return err
	}
	if info.bucketNum, err = conn.readBinaryInt32(); err != nil {
		return err
	}
	if info.num3, err = conn.readUInt(); err != nil {
		return err
	}
	return nil
}
func (info *blockInfo) write(conn *connect) error {
	conn.writeUInt(info.num1)
	conn.Write([]byte{0})
	conn.writeUInt(info.num2)
	binary.Write(conn, binary.LittleEndian, info.bucketNum)
	conn.writeUInt(info.num3)
	return nil
}

type datapacket struct {
	revision     uint
	table        string
	blockInfo    blockInfo
	numRows      uint
	numColumns   uint
	columns      []string
	columnsTypes []string
	rows         [][]driver.Value
}

func (d *datapacket) sendData(conn *connect, numRows uint, values []bytes.Buffer) error {
	if err := conn.writeUInt(ClientDataPacket); err != nil {
		return err
	}
	if d.revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
		if err := conn.writeString(d.table); err != nil {
			return err
		}
	}
	if d.revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
		d.blockInfo.write(conn)
	}
	if err := conn.writeUInt(d.numColumns); err != nil {
		return err
	}
	if err := conn.writeUInt(numRows); err != nil {
		return err
	}
	for i, column := range d.columns {
		if err := conn.writeString(column); err != nil {
			return err
		}
		if err := conn.writeString(d.columnsTypes[i]); err != nil {
			return err
		}

		if _, err := values[i].WriteTo(conn); err != nil {
			return err
		}
	}
	return nil
}

func (ch *clickhouse) datapacket() (*datapacket, error) {
	var (
		err        error
		datapacket = datapacket{
			revision: ch.serverRevision,
		}
	)
	if ch.serverRevision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
		if datapacket.table, err = ch.conn.readString(); err != nil {
			return nil, err
		}
	}
	if ch.serverRevision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
		datapacket.blockInfo.read(ch.conn)
	}
	if datapacket.numColumns, err = ch.conn.readUInt(); err != nil {
		return nil, err
	}
	if datapacket.numRows, err = ch.conn.readUInt(); err != nil {
		return nil, err
	}
	datapacket.rows = make([][]driver.Value, datapacket.numRows)
	values := make([][]driver.Value, datapacket.numColumns)
	for i := 0; i < int(datapacket.numColumns); i++ {
		var column, columnType string
		if column, err = ch.conn.readString(); err != nil {
			return nil, err
		}
		if columnType, err = ch.conn.readString(); err != nil {
			return nil, err
		}
		datapacket.columns = append(datapacket.columns, column)
		datapacket.columnsTypes = append(datapacket.columnsTypes, columnType)
		for index := 0; index < int(datapacket.numRows); index++ {
			v, err := ch.read(columnType)
			if err != nil {
				return nil, err
			}
			values[i] = append(values[i], v)
		}
	}
	for i := 0; i < int(datapacket.numRows); i++ {
		for index := 0; index < int(datapacket.numColumns); index++ {
			datapacket.rows[i] = append(datapacket.rows[i], values[index][i])
		}
	}
	return &datapacket, nil
}
