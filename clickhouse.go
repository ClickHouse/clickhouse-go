package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
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

var (
	splitInsertRe = regexp.MustCompile(`(?i)\sVALUES\s*\(`)
)

type logger func(format string, v ...interface{})

type clickhouse struct {
	logf               logger
	conn               *connect
	serverName         string
	serverRevision     uint64
	serverVersionMinor uint64
	serverVersionMajor uint64
	serverTimezone     *time.Location
	data               *block
	blockSize          int
	inTransaction      bool
}

func (ch *clickhouse) Prepare(query string) (driver.Stmt, error) {
	return ch.prepareContext(context.Background(), query)
}

func (ch *clickhouse) prepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	ch.logf("[prepare] %s", query)
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
		numInput: numInput(query),
	}, nil
}

func (ch *clickhouse) insert(query string) (driver.Stmt, error) {
	if err := ch.sendQuery(splitInsertRe.Split(query, -1)[0] + " VALUES "); err != nil {
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
				numInput: numInput(query),
			}, nil
		case ServerExceptionPacket:
			return nil, ch.exception()
		default:
			return nil, fmt.Errorf("unexpected packet [%d] from server", packet)
		}
	}
}

func (ch *clickhouse) Begin() (driver.Tx, error) {
	return ch.beginTx(context.Background(), txOptions{})
}

type txOptions struct {
	Isolation int
	ReadOnly  bool
}

func (ch *clickhouse) beginTx(ctx context.Context, opts txOptions) (driver.Tx, error) {
	ch.logf("[begin] tx=%t, data=%t", ch.inTransaction, ch.data != nil)
	if ch.inTransaction {
		return nil, sql.ErrTxDone
	}
	if finish := ch.watchCancel(ctx); finish != nil {
		defer finish()
	}
	ch.data = nil
	ch.inTransaction = true
	return ch, nil
}

func (ch *clickhouse) Commit() error {
	ch.logf("[commit] tx=%t, data=%t", ch.inTransaction, ch.data != nil)
	if !ch.inTransaction {
		return sql.ErrTxDone
	}
	defer func() {
		ch.data.reset()
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
	ch.logf("[rollback] tx=%t, data=%t", ch.inTransaction, ch.data != nil)
	if !ch.inTransaction {
		return sql.ErrTxDone
	}
	ch.data = nil
	ch.inTransaction = false
	return ch.cancel()
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
			ch.logf("[got packet] <- exception")
			return ch.exception()
		case ServerProgressPacket:
			progress, err := ch.progress()
			if err != nil {
				return err
			}
			ch.logf("[got packet] <- progress: rows=%d, bytes=%d, total rows=%d",
				progress.bytes,
				progress.rows,
				progress.totalRows,
			)
		case ServerDataPacket:
			var block block
			if err := block.read(ch.serverRevision, ch.conn); err != nil {
				return err
			}
			ch.logf("[got packet] <- data: columns=%d, rows=%d", block.numColumns, block.numRows)
		default:
			return fmt.Errorf("unexpected packet [%d] from server", packet)
		}
		if packet, err = readUvarint(ch.conn); err != nil {
			return err
		}
	}
	return nil
}

func (ch *clickhouse) cancel() error {
	ch.logf("cancel request")
	if err := writeUvarint(ch.conn, ClientCancelPacket); err != nil {
		return err
	}
	return ch.conn.Close()
}

func (ch *clickhouse) watchCancel(ctx context.Context) func() {
	if done := ctx.Done(); done != nil {
		finished := make(chan struct{})
		go func() {
			select {
			case <-done:
				_ = ch.cancel()
				finished <- struct{}{}
				ch.logf("[ch] watchCancel <- done")
			case <-finished:
				ch.logf("[ch] watchCancel <- finished")
			}
		}()
		return func() {
			select {
			case <-finished:
			case finished <- struct{}{}:
			}
		}
	}
	return nil
}
