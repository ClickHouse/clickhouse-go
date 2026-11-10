package clickhouse

import (
	"context"
)

func (h *httpConnect) asyncInsert(ctx context.Context, query string, wait bool, args ...any) error {

	options := queryOptions(ctx)
	options.applyAsyncInsertSettings(wait)
	if len(args) > 0 {
		var err error
		query, err = bindQueryOrAppendParameters(true, &options, query, h.handshake.Timezone, args...)
		if err != nil {
			return err
		}
	}

	res, err := h.sendQuery(ctx, query, &options, nil) //nolint:bodyclose // false positive
	if err != nil {
		return err
	}
	defer discardAndClose(res.Body)

	return nil
}
