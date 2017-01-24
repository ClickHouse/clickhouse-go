package clickhouse

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	ClientHelloPacket  = 0
	ClientQueryPacket  = 1
	ClientDataPacket   = 2
	ClientCancelPacket = 3
	ClientPingPacket   = 4
)

const (
	StateComplete = 2
)

const (
	ServerHelloPacket       = 0
	ServerDataPacket        = 1
	ServerExceptionPacket   = 2
	ServerProgressPacket    = 3
	ServerPongPacket        = 4
	ServerEndOfStreamPacket = 5
	ServerProfileInfoPacket = 6
	ServerTotalsPacket      = 7
	ServerExtremesPacket    = 8
)

var (
	ErrInsertInNotBatchMode = errors.New("insert statement supported only in the batch mode (use begin/commit)")
	ErrLimitDataRequestInTx = errors.New("data request has already been prepared in transaction")
)

type logger func(format string, v ...interface{})

type clickhouse struct {
	log                logger
	conn               *connect
	serverName         string
	serverRevision     uint64
	serverVersionMinor uint64
	serverVersionMajor uint64
	serverTimezone     *time.Location
	inTransaction      bool
	data               *block
}

func (ch *clickhouse) Prepare(query string) (driver.Stmt, error) {
	ch.log("[prepare] %s", query)
	if ch.data != nil {
		return nil, ErrLimitDataRequestInTx
	}
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
	for {
		packet, err := readUvarint(ch.conn)
		if err != nil {
			return nil, err
		}
		switch packet {
		case ServerDataPacket:
			var block block
			if err := block.read(ch.serverRevision, ch.conn); err != nil {
				return nil, err
			}
			ch.data = &block
			return &stmt{
				ch:       ch,
				isInsert: true,
				numInput: strings.Count(query, "?"),
			}, nil
		case ServerExceptionPacket:
			return nil, ch.exception()
		default:
			return nil, fmt.Errorf("unexpected packet [%d] from server", packet)
		}
	}
}

func (ch *clickhouse) Begin() (driver.Tx, error) {
	ch.log("[begin] tx=%t, data=%t", ch.inTransaction, ch.data != nil)
	if ch.inTransaction {
		return nil, sql.ErrTxDone
	}
	ch.data = nil
	ch.inTransaction = true
	return ch, nil
}

func (ch *clickhouse) Commit() error {
	ch.log("[commit] tx=%t, data=%t", ch.inTransaction, ch.data != nil)
	if !ch.inTransaction {
		return sql.ErrTxDone
	}
	defer func() {
		ch.data = nil
		ch.inTransaction = false
	}()
	if ch.data != nil {
		if err := ch.data.write(ch.serverRevision, ch.conn); err != nil {
			return err
		}
		if err := ch.ping(); err != nil {
			return err
		}
		if err := ch.gotPacket(ServerEndOfStreamPacket); err != nil {
			return err
		}
	}
	return nil
}

func (ch *clickhouse) Rollback() error {
	ch.log("[rollback] tx=%t, data=%t", ch.inTransaction, ch.data != nil)
	if !ch.inTransaction {
		return sql.ErrTxDone
	}
	ch.data = nil
	ch.inTransaction = false
	if err := writeUvarint(ch.conn, ClientCancelPacket); err != nil {
		return err
	}
	return ch.ping()
}

func (ch *clickhouse) Close() error {
	ch.data = nil
	return ch.conn.Close()
}

func (ch *clickhouse) gotPacket(p uint64) error {
	packet, err := readUvarint(ch.conn)
	if err != nil {
		return err
	}
	for packet != p {
		switch packet {
		case ServerExceptionPacket:
			ch.log("[got packet] <- exception")
			return ch.exception()
		case ServerProgressPacket:
			progress, err := ch.progress()
			if err != nil {
				return err
			}
			ch.log("[got packet] <- progress: rows=%d, bytes=%d, total rows=%d",
				progress.bytes,
				progress.rows,
				progress.totalRows,
			)
		case ServerDataPacket:
			var block block
			if err := block.read(ch.serverRevision, ch.conn); err != nil {
				return err
			}
			ch.log("[got packet] <- data: columns=%d, rows=%d", block.numColumns, block.numRows)
		default:
			return fmt.Errorf("unexpected packet [%d] from server", packet)
		}
		if packet, err = readUvarint(ch.conn); err != nil {
			return err
		}
	}
	return nil
}
