// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package clickhouse

import (
	"database/sql"
	"fmt"
)

type jwtUpdater interface {
	UpdateJWT(jwt string) error
}

// UpdateSqlJWT is a helper function that updates the JWT within the given sql.DB instance, useful for
// updating expired tokens.
// For the Native interface, the JWT is only updated for new connections.
// For the HTTP interface, the JWT is updated immediately for subsequent requests.
// Existing Native connections are unaffected, but may be forcibly closed by the server upon token expiry.
// For a completely fresh set of connections you should open a new instance.
func UpdateSqlJWT(db *sql.DB, jwt string) error {
	if db == nil {
		return nil
	}

	chDriver, ok := db.Driver().(jwtUpdater)
	if !ok {
		return fmt.Errorf("failed to update JWT: db instance must be ClickHouse")
	}

	return chDriver.UpdateJWT(jwt)
}
