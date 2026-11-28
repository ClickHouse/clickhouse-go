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
		h.refreshCompressionSettings(ctx)
	}
	return nil
}

// refreshCompressionSettings reads readonly again and stops sending the codec settings
// when it is 1. If the read fails, the settings stay off and the server default codec is used.
func (h *httpConnect) refreshCompressionSettings(ctx context.Context) {
	h.compressionSettings = false

	ctx = context.WithValue(ctx, _contextOptionKey, readOnlyQueryOptions(ctx))
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

// readOnlyQueryOptions keeps the per-query options of the SET that apply to its request,
// such as authentication and the session, and drops the ones that belong to the statement.
func readOnlyQueryOptions(ctx context.Context) QueryOptions {
	opt := queryOptions(ctx)
	settings := make(Settings)
	if sessionID, ok := opt.settings["session_id"]; ok {
		settings["session_id"] = sessionID
	}
	opt.settings = settings
	opt.queryID = ""
	opt.async = AsyncOptions{}
	opt.parameters = nil
	opt.external = nil
	opt.columnNamesAndTypes = nil
	opt.events = QueryOptions{}.events
	return opt
}

func isSetStatement(query string) bool {
	query = skipLeadingComments(query)
	if len(query) <= 3 || !strings.EqualFold(query[:3], "SET") {
		return false
	}
	return unicode.IsSpace(rune(query[3]))
}

// skipLeadingComments removes the whitespace and the comments before the first token.
func skipLeadingComments(query string) string {
	for {
		query = strings.TrimLeftFunc(query, unicode.IsSpace)
		n := commentEnd(query)
		if n == 0 {
			return query
		}
		query = query[n:]
	}
}
