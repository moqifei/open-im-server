// Copyright © 2023 OpenIM. All rights reserved.
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

// 注：此处原先有 `_ "net/http/pprof"`。它只会把 pprof 注册到 http.DefaultServeMux，
// 而本进程的 HTTP 入口是 gin（internal/api/init.go），无人服务 DefaultServeMux，
// 因此该导入不产生任何可访问端点，属于死代码。
// pprof 现已由 share.yml 的 debug 段统一提供（pkg/common/debug），默认关闭、只绑回环。
import (
	"github.com/openimsdk/open-im-server/v3/pkg/common/cmd"
	"github.com/openimsdk/tools/system/program"
)

func main() {
	if err := cmd.NewApiCmd().Exec(); err != nil {
		program.ExitWithError(err)
	}
}
