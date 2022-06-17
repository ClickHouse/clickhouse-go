package clickhouse

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

func buildUrl(cfg *httpConfig) *url.URL {
	u := &url.URL{
		Host:   cfg.Host,
		Scheme: cfg.Scheme,
		Path:   "/",
	}
	if len(cfg.User) > 0 {
		if len(cfg.Password) > 0 {
			u.User = url.UserPassword(cfg.User, cfg.Password)
		} else {
			u.User = url.User(cfg.User)
		}
	}

	query := u.Query()
	if len(cfg.Database) > 0 {
		query.Set("database", cfg.Database)
	}
	// todo const
	query.Set("default_format", "TabSeparatedWithNamesAndTypes")

	u.RawQuery = query.Encode()

	return u
}

func newHttpTransport(cfg *httpConfig) *http.Transport {
	// TODO add support tls ...
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   cfg.Timeout,
			DualStack: true,
		}).DialContext,
		MaxIdleConns: 1,
	}
	return transport
}

func (c *httpConnOpener) buildRequest(ctx context.Context, query string) (*http.Request, error) {
	// todo add support compression
	reader := strings.NewReader(query)
	req, err := http.NewRequest(http.MethodPost, c.url.String(), reader)
	if err != nil {
		return nil, fmt.Errorf("error build request: %w", err)
	}
	req.WithContext(ctx)

	return req, nil
}

func (c *httpConnOpener) execQuery(ctx context.Context, query string) (io.ReadCloser, error) {
	transport := c.httpTransport
	req, err := c.buildRequest(ctx, query)

	resp, err := transport.RoundTrip(req)
	if err != nil {
		return nil, fmt.Errorf("execQuery: transport failed to send a request to ClickHouse: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		result := make([]byte, 0)
		buf := bytes.NewBuffer(result)

		_, err = buf.ReadFrom(resp.Body)
		result = buf.Bytes()

		// TODO handle error
		return nil, fmt.Errorf("execQuery: failed to read the response with the status code %d: %s",
			resp.StatusCode, buf.String())
	}

	return resp.Body, nil
}
