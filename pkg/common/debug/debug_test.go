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

package debug

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	conf "github.com/openimsdk/open-im-server/v3/pkg/common/config"
)

// TestPprofMuxServes 验证 pprof 路由可用、未注册 profile 返回 404。
//
// 注意：net/http/pprof 的 init() 会把 handler 注册到 http.DefaultServeMux，
// 这与是否使用空白导入无关（具名导入同样触发 init）。
// 因此"不要把 pprof 暴露到对外端口"的保护措施不在本包，而在
// internal/msggateway/ws_server.go —— 它在对外 WebSocket 端口上使用独立 ServeMux
// 而非 DefaultServeMux。
func TestPprofMuxServes(t *testing.T) {
	srv := httptest.NewServer(pprofMux())
	defer srv.Close()

	for _, path := range []string{
		"/debug/pprof/",
		"/debug/pprof/heap?debug=1",
		"/debug/pprof/goroutine?debug=1",
		"/debug/pprof/allocs?debug=1",
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status=%d", path, resp.StatusCode)
		}
	}

	resp, err := http.Get(srv.URL + "/debug/pprof/not-exist-profile")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown profile: want 404, got %d", resp.StatusCode)
	}
}

// TestDumpHeapOnce 验证快照真的落盘、内容是合法 gzip profile、且轮转与 .tmp 清理正确。
func TestDumpHeapOnce(t *testing.T) {
	dir := t.TempDir()

	// 造一点存活对象，保证 profile 非空。
	hold := make([][]byte, 0, 32)
	for i := 0; i < 32; i++ {
		hold = append(hold, make([]byte, 1<<20))
	}
	runtime.KeepAlive(hold)

	for i := 0; i < 3; i++ {
		if err := dumpHeapOnce(context.Background(), dir, true, 2); err != nil {
			t.Fatalf("dump #%d: %v", i, err)
		}
		time.Sleep(20 * time.Millisecond) // 错开毫秒时间戳
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".tmp" {
			t.Fatalf("stale tmp file left: %s", entry.Name())
		}
		names = append(names, entry.Name())
	}
	if len(names) != 2 {
		t.Fatalf("rotation failed: want 2 files (keep=2), got %d: %v", len(names), names)
	}
	if names[0] >= names[1] {
		t.Fatalf("snapshot names not time-ordered: %v", names)
	}

	latest := filepath.Join(dir, names[1])
	data, err := os.ReadFile(latest)
	if err != nil {
		t.Fatal(err)
	}
	// heap profile 是 gzip 压缩的 protobuf：校验 gzip 魔数。
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		t.Fatalf("snapshot is not a gzip profile, first bytes=% x", data[:min(4, len(data))])
	}

	t.Logf("latest snapshot %s: %d bytes", names[1], len(data))
}

// TestRotate 验证只删最旧的快照，且不误删目录内其他文件。
func TestRotate(t *testing.T) {
	dir := t.TempDir()
	write := func(name string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("keepme.txt")
	for i := 1; i <= 5; i++ {
		write(fmt.Sprintf("%sheap-%02d%s", heapDumpPrefix, i, heapDumpSuffix))
	}

	rotate(dir, 2)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, entry := range entries {
		got = append(got, entry.Name())
	}
	// 期望：2 份最快照 + keepme.txt
	if len(got) != 3 {
		t.Fatalf("want 3 entries, got %d: %v", len(got), got)
	}
	if _, err := os.Stat(filepath.Join(dir, "keepme.txt")); err != nil {
		t.Fatal("rotate deleted an unrelated file")
	}
	if _, err := os.Stat(filepath.Join(dir, heapDumpPrefix+"heap-05"+heapDumpSuffix)); err != nil {
		t.Fatal("rotate deleted the newest snapshot")
	}
	if _, err := os.Stat(filepath.Join(dir, heapDumpPrefix+"heap-01"+heapDumpSuffix)); !os.IsNotExist(err) {
		t.Fatal("rotate did not delete the oldest snapshot")
	}
}

// TestRotateKeepDisabled 验证 keep<=0 时不删任何东西。
func TestRotateKeepDisabled(t *testing.T) {
	dir := t.TempDir()
	name := heapDumpPrefix + "x" + heapDumpSuffix
	if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	rotate(dir, 0)
	if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
		t.Fatal("rotate(0) must not delete files")
	}
}

// TestStartDisabled 验证关闭开关时完全不做任何事（不监听、不 panic）。
func TestStartDisabled(t *testing.T) {
	Start(context.Background(), conf.Debug{Enable: false}, 0)

	// 开启但未配置端口：只记日志，不应 panic。
	Start(context.Background(), conf.Debug{Enable: true}, 0)

	// 端口为 0：跳过监听。
	Start(context.Background(), conf.Debug{Enable: true, Ports: []int{0}}, 0)
}

// TestStartHeapDumpLoop 验证开关生效且 ctx 取消后循环退出。
func TestStartHeapDumpLoop(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	Start(ctx, conf.Debug{
		Enable: true,
		HeapDump: conf.HeapDump{
			Enable:   true,
			Dir:      dir,
			Interval: 30 * time.Millisecond,
			Keep:     2,
		},
	}, 0)

	deadline := time.After(3 * time.Second)
	for {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) > 0 {
			t.Logf("heap dump loop produced %d snapshot(s)", len(entries))
			break
		}
		select {
		case <-deadline:
			t.Fatal("no snapshot produced within 3s")
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
}

// TestRotatePerType 验证 keep 按进程名（服务类型）分组生效，各组互不挤占配额。
func TestRotatePerType(t *testing.T) {
	dir := t.TempDir()
	write := func(name string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// 两类服务各写 5 份（文件名带递增序号以构造时间序）。
	for i := 1; i <= 5; i++ {
		write(fmt.Sprintf("heap-openim-api-%03d.pprof", i))
		write(fmt.Sprintf("heap-openim-rpc-msg-%03d.pprof", i))
	}
	rotate(dir, 2)

	count := func(prefix string) int {
		n := 0
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), prefix) {
				n++
			}
		}
		return n
	}
	if c := count("heap-openim-api-"); c != 2 {
		t.Fatalf("api group: want 2, got %d", c)
	}
	if c := count("heap-openim-rpc-msg-"); c != 2 {
		t.Fatalf("rpc-msg group: want 2, got %d", c)
	}
	if _, err := os.Stat(filepath.Join(dir, "heap-openim-api-001.pprof")); !os.IsNotExist(err) {
		t.Fatal("oldest api snapshot should be deleted")
	}
	if _, err := os.Stat(filepath.Join(dir, "heap-openim-rpc-msg-001.pprof")); !os.IsNotExist(err) {
		t.Fatal("oldest rpc-msg snapshot should be deleted")
	}
}

// TestRotatePerInstance 验证多实例（不同 PID）各自保留 keep 份、互不挤占配额：
// 这样 keep=N 对多实例服务也是"每个实例保留 N 份历史"，而非"全部实例合计 N 份"。
func TestRotatePerInstance(t *testing.T) {
	dir := t.TempDir()
	write := func(name string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// 同一服务 3 个实例（用 100/200/300 模拟 PID），各写 5 份（序号模拟时间序）。
	for i := 1; i <= 5; i++ {
		write(fmt.Sprintf("heap-openim-msgtransfer-100-%03d.pprof", i))
		write(fmt.Sprintf("heap-openim-msgtransfer-200-%03d.pprof", i))
		write(fmt.Sprintf("heap-openim-msgtransfer-300-%03d.pprof", i))
	}
	rotate(dir, 2)

	count := func(prefix string) int {
		n := 0
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), prefix) {
				n++
			}
		}
		return n
	}
	for _, pid := range []string{"100", "200", "300"} {
		if c := count("heap-openim-msgtransfer-" + pid + "-"); c != 2 {
			t.Fatalf("instance %s: want 2, got %d", pid, c)
		}
	}
	// 每个实例最旧的一份应被删除
	for _, pid := range []string{"100", "200", "300"} {
		if _, err := os.Stat(filepath.Join(dir, "heap-openim-msgtransfer-"+pid+"-001.pprof")); !os.IsNotExist(err) {
			t.Fatalf("instance %s: oldest snapshot should be deleted", pid)
		}
	}
}
