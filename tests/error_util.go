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

package tests

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// AssertIsTimeoutError ensures that the error provided is a timeout error.
// It recursively unwraps the provided error and ensures the core error
// implements the Timeout() method and it returns true.
// context deadline error, os deadline error and poll deadline error each
// implement this and return true.
func AssertIsTimeoutError(t *testing.T, err error) {
	assert.True(t, isDeadlineExceededError(err), "error is not a timeout error: %#v", err)
}

type timeout interface {
	Timeout() bool
}

func isDeadlineExceededError(err error) bool {
	nerr, ok := unwrap(err).(timeout)
	if !ok {
		return false
	}

	return nerr.Timeout()
}

// unwrap recursively unwraps the error until it gets the core error
func unwrap(err error) error {
	if uerr := errors.Unwrap(err); uerr != nil {
		return unwrap(uerr)
	}
	return err
}
