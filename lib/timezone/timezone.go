package timezone

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

// fixedOffsetName matches the synthetic fixed-offset timezone names that
// ClickHouse emits for DateTime/DateTime64 columns whose timezone is a
// whole-second offset with no IANA name, e.g. "Fixed/UTC+05:30:15". These names
// are not part of the system tzdata, so time.LoadLocation cannot resolve them.
var fixedOffsetName = regexp.MustCompile(`^Fixed/UTC[+-]\d{2}:[0-5]\d:[0-5]\d$`)

var cache = struct {
	mutex sync.Mutex
	items map[string]*time.Location
}{
	items: make(map[string]*time.Location),
}

func Load(name string) (*time.Location, error) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	if tz, found := cache.items[name]; found {
		return tz, nil
	}
	tz, err := loadLocation(name)
	if err != nil {
		return nil, err
	}
	cache.items[name] = tz
	return tz, nil
}

// loadLocation resolves a ClickHouse timezone name to a *time.Location. Synthetic
// "Fixed/UTC±HH:MM:SS" names are resolved to a fixed-offset zone; every other name
// is delegated to time.LoadLocation for the usual IANA tzdata lookup.
func loadLocation(name string) (*time.Location, error) {
	if fixedOffsetName.MatchString(name) {
		offset, err := time.Parse("-07:00:00", strings.TrimPrefix(name, "Fixed/UTC"))
		if err != nil {
			return nil, fmt.Errorf("clickhouse: invalid fixed-offset timezone %q: %w", name, err)
		}
		_, seconds := offset.Zone()
		return time.FixedZone(name, seconds), nil
	}
	return time.LoadLocation(name)
}
