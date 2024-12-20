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

package chcol

import (
	"testing"
)

func TestVariant_Nil(t *testing.T) {
	v := NewVariant(nil)

	if !v.Nil() {
		t.Fatalf("expected variant to be nil")
	}
}

func TestVariant_Int64(t *testing.T) {
	var in int64 = 42

	v := NewVariant(in)

	out, ok := v.Int64()
	if !ok {
		t.Fatalf("failed to get int64 from variant")
	} else if out != in {
		t.Fatalf("incorrect value from variant. expected: %d got: %d", in, out)
	}
}

func TestVariant_String(t *testing.T) {
	in := "test"

	v := NewVariant(in)

	out, ok := v.String()
	if !ok {
		t.Fatalf("failed to get string from variant")
	} else if out != in {
		t.Fatalf("incorrect value from variant. expected: %s got: %s", in, out)
	}
}

func TestVariant_TypeSwitch(t *testing.T) {
	var in any

	v := NewVariant(in)

	switch v.Any().(type) {
	case int64:
		t.Fatalf("unexpected int64 value from variant")
	case string:
		t.Fatalf("unexpected string value from variant")
	case nil:
	default:
		t.Fatalf("expected nil value from variant")
	}
}
