package clickhouse

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
)

const clickhousePingResponse = "1\n"

// Ping implements the driver.Pinger
func (c *httpConnOpener) Ping(ctx context.Context) error {
	response, err := c.execQuery(ctx, "select 1")
	if err != nil {
		return err
	}
	defer response.Close()

	resp, err := ioutil.ReadAll(response)
	if err != nil {
		return fmt.Errorf("ping: failed to read the response: %w", err)
	}

	if !strings.HasPrefix(string(resp), clickhousePingResponse) {
		return fmt.Errorf("ping: failed to get expected result (1), got '%s' instead", string(resp))
	}

	return nil
}
