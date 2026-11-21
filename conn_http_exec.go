package clickhouse

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"unicode"
)

func (h *httpConnect) exec(ctx context.Context, query string, args ...any) error {
	options := queryOptions(ctx)
	query, err := bindQueryOrAppendParameters(true, &options, query, h.handshake.Timezone, args...)
	if err != nil {
		return err
	}

	res, err := h.sendQuery(ctx, query, &options, nil) //nolint:bodyclose // false positive
	if err != nil {
		return err
	}
	discardAndClose(res.Body)

	// A SET in a session can change readonly, for example SET readonly = 1 (#2150).
	if h.compressionSettings && isSetStatement(query) {
		h.refreshCompressionSettings(ctx, options.settings["session_id"])
	}
	return nil
}

// refreshCompressionSettings reads readonly again and stops sending the codec settings
// when it is 1. If the read fails, the settings stay off and the server default codec is used.
func (h *httpConnect) refreshCompressionSettings(ctx context.Context, sessionID any) {
	h.compressionSettings = false

	opt := QueryOptions{settings: make(Settings)}
	if sessionID != nil {
		opt.settings["session_id"] = sessionID
	}
	ctx = context.WithValue(ctx, _contextOptionKey, opt)
	var readOnly uint8
	rows, err := h.query(ctx, func(nativeTransport, error) {}, "SELECT toUInt8(getSetting('readonly'))")
	if err == nil {
		if rows.Next() {
			err = rows.Scan(&readOnly)
		} else if err = rows.Err(); err == nil {
			err = errors.New("no rows returned for readonly query")
		}
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}
	if err != nil {
		h.logger.Debug("cannot read readonly after SET: network_compression_method and network_zstd_compression_level are not sent",
			slog.Any("error", err))
		return
	}
	h.compressionSettings = readOnly != 1
}

func isSetStatement(query string) bool {
	query = strings.TrimLeftFunc(query, unicode.IsSpace)
	if len(query) <= 3 || !strings.EqualFold(query[:3], "SET") {
		return false
	}
	return unicode.IsSpace(rune(query[3]))
}
