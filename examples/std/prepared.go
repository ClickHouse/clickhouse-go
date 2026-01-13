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
	"database/sql"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// PreparedSelect demonstrates using database/sql prepared statements for read queries.
func PreparedSelect() error {
	conn, err := GetStdOpenDBConnection(clickhouse.Native, nil, nil, nil)
	if err != nil {
		return err
	}
	defer func(db *sql.DB) { _ = db.Close() }(conn)

	ctx := context.Background()
	if err := conn.PingContext(ctx); err != nil {
		return err
	}

	stmt, err := conn.PrepareContext(ctx, "SELECT ? + ?")
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	rows, err := stmt.QueryContext(ctx, 2, 3)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return fmt.Errorf("no rows returned from prepared SELECT")
	}
	var sum int64
	if err := rows.Scan(&sum); err != nil {
		return err
	}
	if sum != 5 {
		return fmt.Errorf("unexpected result from prepared SELECT: got %d, want 5", sum)
	}
	return rows.Err()
}
