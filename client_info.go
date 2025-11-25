package clickhouse

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"runtime"
	"sort"
	"strings"
)

const ClientName = "clickhouse-go"

const (
	ClientVersionMajor       = 2
	ClientVersionMinor       = 41
	ClientVersionPatch       = 0
	ClientTCPProtocolVersion = proto.DBMS_TCP_PROTOCOL_VERSION
)

type ClientInfo struct {
	Products []struct {
		Name    string
		Version string
	}

	comment []string
}

func (o ClientInfo) String() string {
	var s strings.Builder

	info := o

	info.Products = append(info.Products, struct{ Name, Version string }{
		Name:    ClientName,
		Version: fmt.Sprintf("%d.%d.%d", ClientVersionMajor, ClientVersionMinor, ClientVersionPatch),
	})

	encodedProducts := make([]string, len(info.Products))
	for i, product := range info.Products {
		encodedProducts[i] = fmt.Sprintf("%s/%s", product.Name, product.Version)
	}
	s.WriteString(strings.Join(encodedProducts, " "))

	lvMeta := "lv:go/" + runtime.Version()[2:]
	osMeta := "os:" + runtime.GOOS

	chunks := append(info.comment, lvMeta, osMeta) // nolint:gocritic

	s.WriteByte(' ')
	s.WriteByte('(')
	s.WriteString(strings.Join(chunks, "; "))
	s.WriteByte(')')

	return s.String()
}

func mapKeysInOrder[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	return keys
}
