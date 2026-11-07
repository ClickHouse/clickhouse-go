package clickhouse

import (
	"context"
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
	// Exec answers 200 with an empty body on success. A failure after the
	// status line was flushed shows up as an in-band "__exception__" block
	// and/or X-ClickHouse-Exception-Code — same as insert/batch, not as a
	// non-200 status. Discarding the body here used to report those as nil.
	return h.insertResponseError(res)
}
