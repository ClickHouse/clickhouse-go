package clickhouse

import (
	"net/url"
	"time"
)

// httpConfig - config for http driver
type httpConfig struct {
	Debug    bool
	Host     string
	Scheme   string
	User     string
	Password string
	Database string
	Timeout  time.Duration
	location *time.Location
}

func parseHttpPDsn(dsn string) (*httpConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	cfg := &httpConfig{
		location: time.UTC,
	}

	cfg.Scheme, cfg.Host = u.Scheme, u.Host
	// TODO fix this
	if len(u.Path) > 1 {
		// skip '/'
		cfg.Database = u.Path[1:]
	}
	if u.User != nil {
		cfg.User = u.User.Username()
		if passwd, ok := u.User.Password(); ok {
			cfg.Password = passwd
		}
	}

	for k, v := range u.Query() {
		switch k {
		case "location":
			cfg.location, err = time.LoadLocation(v[0])
		}
	}

	return cfg, nil
}
