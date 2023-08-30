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
	"testing"

	econf "github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
)

func TestIotdb(t *testing.T) {
	contextLogger := econf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	sink := &iotdbSink{
		addr:     "localhost",
		port:     "6667",
		deviceId: "root.ln.test",
		user:     "root",
		passwd:   "root",
	}

	sink.Configure(map[string]interface{}{
		"addr":     "localhost",
		"port":     "6667",
		"deviceId": "root.ln.test",
		"user":     "root",
		"passwd":   "root",
	})
	err := sink.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	data := []map[string]interface{}{
		{"timestamp": 1, "name": "John", "age": 43, "mobile": "334433"},
		{"timestamp": 2, "name": "Susan", "age": 34, "mobile": "334433"},
		{"timestamp": 3, "name": "Susan", "age": 34, "mobile": "334433"},
	}

	for _, d := range data {
		err = sink.Collect(ctx, d)
		if err != nil {
			t.Error(err)
			return
		}
	}
	sink.Close(ctx)
}
