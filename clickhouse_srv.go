package clickhouse

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
)

// srvService returns the DNS SRV service label for a given Options.
// Native uses "clickhouse"; HTTP uses "clickhouse-http" or "clickhouse-https"
// depending on whether TLS is configured.
func srvService(opt *Options) string {
	if opt.Protocol == HTTP {
		if opt.TLS != nil {
			return "clickhouse-https"
		}
		return "clickhouse-http"
	}
	return "clickhouse"
}

// srvLookupName derives the SRV record name from an Addr entry.
// Any ":port" is stripped. A leading "_" means the entry already names a full
// SRV record (for example "_clickhouse._tcp.example.com") and is looked up
// verbatim via LookupSRV("", "", name).
// Otherwise the lookup uses service/proto so that LookupSRV composes
// `_<service>._<proto>.<host>`.
func srvLookupName(addr string, opt *Options) (service, proto, name string) {
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	if strings.HasPrefix(host, "_") {
		return "", "", host
	}
	return srvService(opt), "tcp", host
}

// resolveAddr returns the final list of addresses to dial. When
// opt.ResolveSRV is false it returns opt.Addr unchanged. Otherwise every
// entry of opt.Addr is treated as a DNS SRV name and resolved. Results are
// sorted by Priority ascending then Weight descending (stable) per entry and
// concatenated in the order of opt.Addr. An empty total returns
// ErrAcquireConnNoAddress.
func resolveAddr(ctx context.Context, opt *Options) ([]string, error) {
	if !opt.ResolveSRV {
		return opt.Addr, nil
	}

	lookup := opt.lookupSRV
	if lookup == nil {
		lookup = net.DefaultResolver.LookupSRV
	}

	var out []string
	for _, addr := range opt.Addr {
		service, proto, name := srvLookupName(addr, opt)
		_, records, err := lookup(ctx, service, proto, name)
		if err != nil {
			return nil, fmt.Errorf("clickhouse: resolve SRV %q: %w", name, err)
		}
		sort.SliceStable(records, func(i, j int) bool {
			if records[i].Priority != records[j].Priority {
				return records[i].Priority < records[j].Priority
			}
			return records[i].Weight > records[j].Weight
		})
		for _, r := range records {
			target := strings.TrimSuffix(r.Target, ".")
			if target == "" {
				// RFC 2782: a target of "." means the service is not available.
				continue
			}
			out = append(out, net.JoinHostPort(target, strconv.Itoa(int(r.Port))))
		}
	}

	if len(out) == 0 {
		return nil, ErrAcquireConnNoAddress
	}
	return out, nil
}
