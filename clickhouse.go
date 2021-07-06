package clickhouse

import (
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"github.com/ClickHouse/clickhouse-go/lib/column"
	"github.com/ClickHouse/clickhouse-go/lib/data"
	"github.com/ClickHouse/clickhouse-go/lib/protocol"
	"github.com/ClickHouse/clickhouse-go/lib/types"
)

type (
	Date     = types.Date
	DateTime = types.DateTime
	UUID     = types.UUID
)

type ExternalTable struct {
	Name    string
	Values  [][]driver.Value
	Columns []column.Column
}

var (
	ErrInsertInNotBatchMode = errors.New("insert statement supported only in the batch mode (use begin/commit)")
	ErrLimitDataRequestInTx = errors.New("data request has already been prepared in transaction")
)

var (
	splitInsertRe = regexp.MustCompile(`(?i)\sVALUES\s*\(`)
)

type logger func(format string, v ...interface{})

type clickhouse struct {
	sync.Mutex
	data.ServerInfo
	data.ClientInfo
	logf          logger
	conn          *connect
	block         *data.Block
	buffer        *bufio.Writer
	decoder       *binary.Decoder
	encoder       *binary.Encoder
	settings      *querySettings
	compress      bool
	blockSize     int
	inTransaction bool
}

func (ch *clickhouse) Prepare(query string) (driver.Stmt, error) {
	return ch.prepareContext(context.Background(), query)
}

func (ch *clickhouse) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return ch.prepareContext(ctx, query)
}

func (ch *clickhouse) prepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	var err error
	ch.logf("[prepare] server=%s %s", ch.conn.Conn.RemoteAddr().String(), query)
	defer func() {
		if err != nil {
			ch.logf("[prepare] server=%s %s error=%s", ch.conn.Conn.RemoteAddr().String(), query, err.Error())
		}
	}()

	switch {
	case ch.conn.closed:
		err = driver.ErrBadConn
		return nil, err
	case ch.block != nil:
		err = ErrLimitDataRequestInTx
		return nil, err
	case isInsert(query):
		if !ch.inTransaction {
			err = ErrInsertInNotBatchMode
			return nil, err
		}
		var stmt driver.Stmt
		stmt, err = ch.insert(ctx, query)
		return stmt, err
	}
	return &stmt{
		ch:       ch,
		query:    query,
		numInput: numInput(query),
	}, nil
}

func (ch *clickhouse) insert(ctx context.Context, query string) (_ driver.Stmt, err error) {
	if err := ch.sendQuery(ctx, splitInsertRe.Split(query, -1)[0]+" VALUES ", nil); err != nil {
		return nil, err
	}
	if ch.block, err = ch.readMeta(); err != nil {
		return nil, err
	}
	return &stmt{
		ch:       ch,
		isInsert: true,
	}, nil
}

func (ch *clickhouse) Begin() (driver.Tx, error) {
	return ch.beginTx(context.Background(), txOptions{})
}

func (ch *clickhouse) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return ch.beginTx(ctx, txOptions{
		Isolation: int(opts.Isolation),
		ReadOnly:  opts.ReadOnly,
	})
}

type txOptions struct {
	Isolation int
	ReadOnly  bool
}

func (ch *clickhouse) beginTx(ctx context.Context, opts txOptions) (*clickhouse, error) {
	var err error
	ch.logf("[begin] server=%s tx=%t, data=%t", ch.conn.Conn.RemoteAddr().String(), ch.inTransaction, ch.block != nil)
	defer func() {
		if err != nil {
			ch.logf("[begin] server=%s tx=%t, data=%t error=%s", ch.conn.Conn.RemoteAddr().String(), ch.inTransaction, ch.block != nil, err.Error())
		}
	}()

	switch {
	case ch.inTransaction:
		err = sql.ErrTxDone
		return nil, err
	case ch.conn.closed:
		err = driver.ErrBadConn
		return nil, err
	}
	if finish := ch.watchCancel(ctx); finish != nil {
		defer finish()
	}
	ch.block = nil
	ch.inTransaction = true
	return ch, nil
}

func (ch *clickhouse) Commit() error {
	var err error
	ch.logf("[commit] server=%s tx=%t, data=%t", ch.conn.Conn.RemoteAddr().String(), ch.inTransaction, ch.block != nil)
	defer func() {
		if err != nil {
			ch.logf("[commit] server=%s tx=%t, data=%t error=%s", ch.conn.Conn.RemoteAddr().String(), ch.inTransaction, ch.block != nil, err.Error())
		}
	}()

	defer func() {
		if ch.block != nil {
			ch.block.Reset()
			ch.block = nil
		}
		ch.inTransaction = false
	}()
	switch {
	case !ch.inTransaction:
		err = sql.ErrTxDone
		return err
	case ch.conn.closed:
		err = driver.ErrBadConn
		return err
	}
	if ch.block != nil {
		if err = ch.writeBlock(ch.block, ""); err != nil {
			return err
		}
		// Send empty block as marker of end of data.
		if err = ch.writeBlock(&data.Block{}, ""); err != nil {
			return err
		}
		if err = ch.encoder.Flush(); err != nil {
			return err
		}
		err = ch.process()
		return err
	}
	return nil
}

func (ch *clickhouse) Rollback() error {
	var err error
	ch.logf("[rollback] server=%s tx=%t, data=%t", ch.conn.Conn.RemoteAddr().String(), ch.inTransaction, ch.block != nil)
	defer func() {
		if err != nil {
			ch.logf("[rollback] server=%s tx=%t, data=%t error=%s", ch.conn.Conn.RemoteAddr().String(), ch.inTransaction, ch.block != nil, err.Error())
		}
	}()

	if !ch.inTransaction {
		err = sql.ErrTxDone
		return err
	}
	if ch.block != nil {
		ch.block.Reset()
	}
	ch.block = nil
	ch.buffer = nil
	ch.inTransaction = false
	err = ch.conn.Close()
	return err
}

func (ch *clickhouse) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case ExternalTable, column.IP, column.UUID:
		return nil
	case nil, []byte, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, string, time.Time:
		return nil
	}
	switch v := nv.Value.(type) {
	case
		[]int, []int8, []int16, []int32, []int64,
		[]uint, []uint8, []uint16, []uint32, []uint64,
		[]float32, []float64,
		[]string:
		return nil
	case net.IP, *net.IP:
		return nil
	case driver.Valuer:
		value, err := v.Value()
		if err != nil {
			return err
		}
		nv.Value = value
	default:
		switch value := reflect.ValueOf(nv.Value); value.Kind() {
		case reflect.Slice:
			return nil
		case reflect.Bool:
			nv.Value = uint8(0)
			if value.Bool() {
				nv.Value = uint8(1)
			}
		case reflect.Int8:
			nv.Value = int8(value.Int())
		case reflect.Int16:
			nv.Value = int16(value.Int())
		case reflect.Int32:
			nv.Value = int32(value.Int())
		case reflect.Int64:
			nv.Value = value.Int()
		case reflect.Uint8:
			nv.Value = uint8(value.Uint())
		case reflect.Uint16:
			nv.Value = uint16(value.Uint())
		case reflect.Uint32:
			nv.Value = uint32(value.Uint())
		case reflect.Uint64:
			nv.Value = uint64(value.Uint())
		case reflect.Float32:
			nv.Value = float32(value.Float())
		case reflect.Float64:
			nv.Value = float64(value.Float())
		case reflect.String:
			nv.Value = value.String()
		}
	}
	return nil
}

func (ch *clickhouse) Close() error {
	var err error
	ch.logf("[close] server=%s", ch.conn.Conn.RemoteAddr())
	defer func() {
		if err != nil {
			ch.logf("[close] server=%s error=%s", ch.conn.Conn.RemoteAddr(), err.Error())
		}
	}()

	ch.block = nil
	err = ch.conn.Close()
	return err
}

func (ch *clickhouse) process() error {
	packet, err := ch.decoder.Uvarint()
	if err != nil {
		return err
	}
	for {
		switch packet {
		case protocol.ServerPong:
			ch.logf("[process] <- pong")
			return nil
		case protocol.ServerException:
			ch.logf("[process] <- exception")
			return ch.exception()
		case protocol.ServerProgress:
			progress, err := ch.progress()
			if err != nil {
				return err
			}
			ch.logf("[process] <- progress: rows=%d, bytes=%d, total rows=%d",
				progress.rows,
				progress.bytes,
				progress.totalRows,
			)
		case protocol.ServerProfileInfo:
			profileInfo, err := ch.profileInfo()
			if err != nil {
				return err
			}
			ch.logf("[process] <- profiling: rows=%d, bytes=%d, blocks=%d", profileInfo.rows, profileInfo.bytes, profileInfo.blocks)
		case protocol.ServerData:
			block, err := ch.readBlock()
			if err != nil {
				return err
			}
			ch.logf("[process] <- data: packet=%d, columns=%d, rows=%d", packet, block.NumColumns, block.NumRows)
		case protocol.ServerEndOfStream:
			ch.logf("[process] <- end of stream")
			return nil
		default:
			ch.conn.Close()
			return fmt.Errorf("[process] unexpected packet [%d] from server", packet)
		}
		if packet, err = ch.decoder.Uvarint(); err != nil {
			return err
		}
	}
}

func (ch *clickhouse) cancel() error {
	ch.logf("[cancel request]")
	// even if we fail to write the cancel, we still need to close
	err := ch.encoder.Uvarint(protocol.ClientCancel)
	if err == nil {
		err = ch.encoder.Flush()
	}
	// return the close error if there was one, otherwise return the write error
	if cerr := ch.conn.Close(); cerr != nil {
		return cerr
	}
	return err
}

func (ch *clickhouse) watchCancel(ctx context.Context) func() {
	if done := ctx.Done(); done != nil {
		finished := make(chan struct{})
		go func() {
			select {
			case <-done:
				ch.cancel()
				finished <- struct{}{}
				ch.logf("[cancel] <- done")
			case <-finished:
				ch.logf("[cancel] <- finished")
			}
		}()
		return func() {
			select {
			case <-finished:
			case finished <- struct{}{}:
			}
		}
	}
	return func() {}
}

func (ch *clickhouse) ExecContext(ctx context.Context, query string,
	args []driver.NamedValue) (driver.Result, error) {
	finish := ch.watchCancel(ctx)
	defer finish()
	stmt, err := ch.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	dargs := make([]driver.Value, len(args))
	for i, nv := range args {
		dargs[i] = nv.Value
	}
	return stmt.Exec(dargs)
}
