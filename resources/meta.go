package resources

import "github.com/ClickHouse/clickhouse-go/v2/lib/proto"

// MinSupportedVersion is the minimum ClickHouse server version supported by this driver.
var MinSupportedVersion = proto.Version{Major: 25, Minor: 8, Patch: 0}
