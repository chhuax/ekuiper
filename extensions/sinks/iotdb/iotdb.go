// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/iotdb-client-go/client"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type iotdbSink struct {
	nodeUrls     string
	user         string
	passwd       string
	deviceId     string
	measurements string
	dataTypes    string
	sessionPool  client.SessionPool
}

func (m *iotdbSink) Configure(props map[string]interface{}) error {
	if i, ok := props["nodeUrls"]; ok {
		if i, ok := i.(string); ok {
			m.nodeUrls = i
		}
	}
	if i, ok := props["user"]; ok {
		if i, ok := i.(string); ok {
			m.user = i
		}
	}
	if i, ok := props["passwd"]; ok {
		if i, ok := i.(string); ok {
			m.passwd = i
		}
	}
	if i, ok := props["deviceId"]; ok {
		if i, ok := i.(string); ok {
			m.deviceId = i
		}
	}
	if i, ok := props["measurements"]; ok {
		if i, ok := i.(string); ok {
			m.measurements = i
		}
	}
	if i, ok := props["dataTypes"]; ok {
		if i, ok := i.(string); ok {
			m.dataTypes = i
		}
	}

	return nil
}

func (m *iotdbSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	logger.Debugln("Opening iotdb Sink")

	config := &client.PoolConfig{
		NodeUrls: strings.Split(m.nodeUrls, ","),
		UserName: m.user,
		Password: m.passwd,
	}
	m.sessionPool = client.NewSessionPool(config, 3, 60000, 60000, false)
	return nil
}

func (m *iotdbSink) Collect(ctx api.StreamContext, data interface{}) error {
	logger := ctx.GetLogger()
	logger.Infof("start collect data")

	switch t := data.(type) {
	case map[string]interface{}:

		m.insertIotdb(ctx, data)
	case []map[string]interface{}:
		for _, k := range t {
			m.insertIotdb(ctx, k)
		}
	}

	return nil
}

func (m *iotdbSink) insertIotdb(ctx api.StreamContext, data interface{}) (err error) {
	logger := ctx.GetLogger()
	jsonBytes, err := json.Marshal(&data)
	if err != nil {
		return err
	}

	d := make(map[string]interface{})
	err = json.Unmarshal(jsonBytes, &d)
	if err != nil {
		return fmt.Errorf("fail to decode data %s after applying dataTemplate for error %v", string(jsonBytes), err)
	}

	session, err := m.sessionPool.GetSession()
	if err != nil {
		logger.Errorf("session pool get session error!")
		return err
	}
	keys := make([]string, 0, len(d)-1)
	values := make([]interface{}, 0, len(d)-1)
	types := make([]client.TSDataType, 0, len(d)-1)
	timestamp := int64(d["timestamp"].(float64))
	for k := range d {
		if k == "timestamp" {
			continue
		}
		keys = append(keys, k)
		value := d[k]
		values = append(values, value)
		t := transformType(value)
		types = append(types, t)
	}
	var (
		deviceId     = m.deviceId
		measurements = keys
		dataTypes    = types
	)
	deviceId, err = ctx.ParseTemplate(m.deviceId, d)
	if err != nil {
		logger.Errorf("parse template for table %s error: %v", m.deviceId, err)
		return err
	}
	defer m.sessionPool.PutBack(session)
	if err == nil {
		_, err := session.InsertRecord(deviceId, measurements, dataTypes, values, timestamp)
		if err != nil {
			logger.Errorf("session insertRecord err %v", err)
		}
	}
	return err
}

func transformType(value interface{}) (dt client.TSDataType) {
	switch vt := value.(type) {
	case int8, int16, int32:
		dt = client.INT32
	case int64:
		dt = client.INT64
	case float32:
		dt = client.FLOAT
	case float64:
		dt = client.DOUBLE
	case string:
		dt = client.TEXT
	case bool:
		dt = client.BOOLEAN
	default:
		fmt.Errorf("change to tsDataType UNKNOWN value : %v, type : %v", value, vt)
		dt = client.UNKNOWN
	}
	return dt
}

func (m *iotdbSink) Close(ctx api.StreamContext) error {
	m.sessionPool.Close()
	return nil
}

func Iotdb() api.Sink {
	return &iotdbSink{}
}
