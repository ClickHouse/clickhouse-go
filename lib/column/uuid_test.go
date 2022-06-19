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

package column

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
)

func getTestUuids() (uuids []uuid.UUID, err error) {
	uuid1, err := uuid.Parse("603966d6-ed93-11ec-8ea0-0242ac120002")
	if err != nil {
		return
	}
	uuid2, err := uuid.Parse("60396956-ed93-11ec-8ea0-0242ac120002")
	if err != nil {
		return
	}

	uuids = []uuid.UUID{uuid1, uuid2}
	return
}

func TestUuid_ScanRow(t *testing.T) {
	uuids, err := getTestUuids()
	if err != nil {
		t.Fatal(err)
	}

	col := UUID{}
	_, err = col.Append(uuids)
	if err != nil {
		t.Fatal(err)
	}

	// scanning uuid.UUID
	for i := range uuids {
		var u uuid.UUID
		err := col.ScanRow(&u, i)
		if err != nil {
			require.Error(t, err, "unexpected ScanRow error")
		}
		if u != uuids[i] {
			require.Failf(t, "Invalid result of ScanRow", "ScanRow resulted in %q instead of %q", u, uuids[i])
		}
	}

	// scanning strings
	for i := range uuids {
		var u string
		err := col.ScanRow(&u, i)
		if err != nil {
			require.Error(t, err, "unexpected ScanRow error")
		}
		if u != uuids[i].String() {
			require.Failf(t, "Invalid result of ScanRow", "ScanRow resulted in %q instead of %q", u, uuids[i])
		}
	}
}
