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
	in := struct{}{}

	v := NewVariant(in)

	if i, ok := v.Int64(); ok {
		t.Fatalf("unexpected int64 value from variant: %d", i)
	} else if s, ok := v.String(); ok {
		t.Fatalf("unexpected string value from variant: %s", s)
	}
}
