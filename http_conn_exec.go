package clickhouse

import (
	"context"
	"database/sql/driver"
)

func (c *httpConnOpener) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	body, err := bind(c.location, query, rebind(args)...)
	response, err := c.execQuery(ctx, body)
	if err != nil {
		return nil, err
	}
	defer response.Close()

	return driver.RowsAffected(0), nil
}
