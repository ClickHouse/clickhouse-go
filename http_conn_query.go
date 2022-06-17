package clickhouse

import (
	"context"
	"database/sql/driver"
)

func (c *httpConnOpener) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	body, err := bind(c.location, query, rebind(args)...)

	response, err := c.execQuery(ctx, body)
	if err != nil {
		return nil, err
	}

	res, err := newTextRows(c, response)
	if err != nil {
		return nil, err
	}

	return res, nil
}
