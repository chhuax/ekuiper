// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"net/http"
	"strings"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

/**
 **	A function which will check url is ok
 ** There are 2 arguments:
 **  0: checkedUrl, the url need to check
 **  1: urlMethod, the http method
 **/

type urlCheckFunc struct{}

func (f *urlCheckFunc) Validate(args []interface{}) error {
	if len(args) != 2 {
		return fmt.Errorf("urlCheck function only supports 2 parameter but got %d", len(args))
	}
	if arg1, ok := args[1].(ast.Expr); ok {
		if _, ok := arg1.(*ast.StringLiteral); !ok {
			return fmt.Errorf("the second parameter of urlCheck function must be a string literal")
		}
	}
	return nil
}

func (f *urlCheckFunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	// logger := ctx.GetLogger()
	fmt.Printf("Exec check")
	checkedUrl, ok := args[0].(string)
	if !ok {
		// logger.Debugf("Exec urlCheckFunc with arg0 %s", checkedUrl)
		return fmt.Errorf("args[0] is not a string, got %v", args[0]), false
	}

	urlMethod, ok := args[1].(string)
	if !ok {
		// logger.Debugf("Exec urlCheckFunc with arg1 %s", urlMethod)
		return fmt.Errorf("args[1] is not a string, got %v", args[0]), false
	}
	switch urlMethod {
	case "get", "GET", "Get":
		_, err := http.Get(checkedUrl)
		if err != nil {
			// logger.Infof("Exec urlCheckFunc get has error %v", err)
			return fmt.Errorf("check url failed, err: %v", err), false
		}
		return nil, true
	case "head", "HEAD", "Head":
		_, err := http.Head(checkedUrl)
		if err != nil {
			// logger.Infof("Exec urlCheckFunc head has error %v", err)
			return fmt.Errorf("check url failed, err: %v", err), false
		}
		return nil, true
	case "post", "POST", "Post":
		req_data := `{}`
		req, err := http.NewRequest("POST", checkedUrl, strings.NewReader(req_data))
		if err != nil {
			// logger.Warnf("Exec urlCheckFunc post has error %v", err)
			return fmt.Errorf("check url failed, err: %v", err), false
		}
		client := &http.Client{}
		_, err = client.Do(req)
		if err != nil {
			// logger.Warnf("Exec urlCheckFunc post has error %v", err)
			return fmt.Errorf("check url failed, err: %v", err), false
		}
		return nil, true
	default:
		return fmt.Errorf("check url failed, not supported method: %s", urlMethod), false
	}

}

func (f *urlCheckFunc) IsAggregate() bool {
	return false
}

func AccumulateUrlCheck() api.Function {
	return &urlCheckFunc{}
}
