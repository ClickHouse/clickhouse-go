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

package std

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func EndOfProcessAndGotData() error {
	conn, err := GetStdOpenDBConnection(clickhouse.Native, clickhouse.Settings{
		"send_logs_level": "trace",
	}, nil, nil)
	if err != nil {
		return err
	}
	var totalBlocks int
	// use context to pass a call back for end of process and got data
	ctx := clickhouse.Context(context.Background(), clickhouse.WithEndOfProcess(func() {
		fmt.Println("process is finished")
	}), clickhouse.WithGotData(func() {
		totalBlocks++
	}))

	rows, err := conn.QueryContext(ctx, "SELECT number from numbers(1000000) LIMIT 1000000")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
	}

	fmt.Printf("Total data blocks: %d\n", totalBlocks)
	return rows.Err()
}
