// Copyright 2022-2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckUrlFunction(t *testing.T) {
	urlCheckFunc := urlCheckFunc{}
	args := []interface{}{"http://www.baidu.com", "get"}
	r, b := urlCheckFunc.Exec(args, nil)
	if !b {
		fmt.Printf(" check not ok %v", r)
	}
	assert.Equal(t, b, true)
}
