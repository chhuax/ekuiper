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
	"strings"
	"testing"

	econf "github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/stretchr/testify/assert"
)

func TestIotdbSinkSingle(t *testing.T) {
	contextLogger := econf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	sink := initIotdbSink()
	err := sink.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	data := []map[string]interface{}{
		{"time": 2, "name": "Susan", "age": 34, "mobile": "334433", "meta": "adfad.afdafds2", "device": "root.ln.test.property"},
		{"time": 3, "name": "Susan", "age": 34, "mobile": "334433", "meta": "adfad.afdafds3", "device": "root.ln.test.d1"},
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

func TestIotdbSinkMultiple(t *testing.T) {
	contextLogger := econf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	sink := initIotdbSink()
	err := sink.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	data := []map[string]interface{}{
		{"time": 1, "name": "John", "msg": "334433"},
	}

	err = sink.Collect(ctx, data)
	if err != nil {
		t.Error(err)
	}
	sink.Close(ctx)
}

func TestTempalte(t *testing.T) {
	contextLogger := econf.Log.WithField("rule", "test")
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	sink := initIotdbSink()
	err := sink.Open(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	data := []map[string]interface{}{
		{"time": 1, "name": "John", "age": 43, "mobile": "334433"},
		{"time": 2, "name": "Susan", "age": 34, "mobile": "334433"},
		{"time": 3, "name": "Susan", "age": 34, "mobile": "334433"},
	}

	deviceId, _ := ctx.ParseTemplate("hello, {{if gt .age 40}}{{.name}}{{else}}{{.mobile}}{{end}}", data[0])
	deviceId2, _ := ctx.ParseTemplate("{{printf \"%.3s\" .name}}", data[1])
	assert.Equal(t, "hello, John", deviceId)
	assert.Equal(t, "Sus", deviceId2)

}

func initIotdbSink() (sink *iotdbSink) {

	sink = &iotdbSink{}

	sink.Configure(map[string]interface{}{
		"nodeUrls": "172.20.45.128:6668",
		"deviceId": "{{.device}}.metric",
		"user":     "root",
		"passwd":   "root",
	})
	return sink
}

func TestRegexTopic(t *testing.T) {
	str := "root/ln/wf01/`abc.d`"
	strExpect := "root.ln.wf01.`abc.d`"
	newStr := revertTopic(str)
	assert.Equal(t, newStr, strExpect)

	str2 := "root/ln/wf01/`abc.d`/deviceId"
	str2Expect := "root.ln.wf01.`abc.d`.deviceId"
	newStr2 := revertTopic(str2)
	assert.Equal(t, newStr2, str2Expect)

	str3 := "root/ln/wf01/`abc.d`/deviceId/`dafd.w`"
	str3Expect := "root.ln.wf01.`abc.d`.deviceId.`dafd.w`"
	newStr3 := revertTopic(str3)
	assert.Equal(t, newStr3, str3Expect)
}

func TestReplaceLastStr(t *testing.T) {
	deviceId := "root.ln.wf01.wd03.property.metric"
	index := strings.LastIndex(deviceId, ".property.metric")
	if index != -1 {
		deviceId = deviceId[:index] + ".metric"
	}
	assert.Equal(t, deviceId, "root.ln.wf01.wd03.metric")
}
