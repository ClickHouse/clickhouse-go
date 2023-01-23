// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
	ClientVersionMinor       = 5
	ClientVersionPatch       = 1
	ClientTCPProtocolVersion = proto.DBMS_TCP_PROTOCOL_VERSION
)

type ClientInfo struct {
	Products []struct {
		Name    string
		Version string
	}

	Comment []string
	Meta    map[string]string
}

func (i ClientInfo) String() string {
	var s strings.Builder

	products := append(i.Products, struct{ Name, Version string }{
		Name:    ClientName,
		Version: fmt.Sprintf("%d.%d.%d", ClientVersionMajor, ClientVersionMinor, ClientVersionPatch),
	})

	for _, product := range products {
		s.WriteString(product.Name)
		s.WriteByte('/')
		s.WriteString(product.Version)
		s.WriteByte(' ')
	}

	if i.Meta == nil {
		i.Meta = make(map[string]string)
	}
	i.Meta["lv"] = "go/" + runtime.Version()[2:]
	i.Meta["os"] = runtime.GOOS

	totalChunks := len(i.Comment) + len(i.Meta)
	if totalChunks == 0 {
		return strings.TrimSpace(s.String())
	}
	var chunksWritten int
	writePart := func() {
		if chunksWritten == totalChunks-1 {
			return
		}

		s.WriteByte(';')
		s.WriteByte(' ')
		chunksWritten++
	}

	s.WriteByte('(')

	for _, comment := range i.Comment {
		s.WriteString(comment)
		writePart()
	}

	for _, key := range mapKeysInOrder(i.Meta) {
		s.WriteString(key)
		s.WriteByte(':')
		s.WriteString(i.Meta[key])
		writePart()
	}

	s.WriteByte(')')

	return s.String()
}

func mapKeysInOrder[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key, _ := range m {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	return keys
}
