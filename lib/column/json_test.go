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
	"reflect"
	"testing"
	"time"
)

// will got new map to different order
func getMapRefByMapForTest(myMap map[string]interface{}) reflect.Value {
	newMap := map[string]interface{}{}
	for k := range myMap {
		newMap[k] = myMap[k]
	}

	return reflect.ValueOf(newMap)
}

func Test_iterateMap(t *testing.T) {
	myMap := map[string]interface{}{
		"col1": int64(1),
		"col2": time.Date(2022, 11, 21, 16, 21, 0, 0, time.Local),
		"col3": nil,
		"col4": "1",
	}

	col := &JSONObject{
		columns:  []JSON{},
		name:     "MyJson",
		root:     true,
		encoding: 0,
	}

	preFill := 0
	for i := 0; i < 1000; i++ {
		newMap := getMapRefByMapForTest(myMap)
		err := iterateMap(newMap, col, preFill)
		if err != nil {
			t.Errorf("iterateMap got err: %v", err)
		}
	}

	for _, v := range col.columns {
		if v.Rows() != 1000 {
			t.Errorf("iterateMap unstable!")
		}
	}
}
