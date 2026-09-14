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

// Package debug 提供线上诊断能力：pprof HTTP 端点 + 定时 heap 快照。
//
// 两类能力：
//  1. pprof HTTP 端点（/debug/pprof/*）：按需抓取 goroutine / heap / CPU profile；
//  2. 定时 heap 快照：把 heap profile 定时落盘到本地目录并只保留最近 N 份，
//     用于 OOM 事后分析——不必在进程濒死时冒险现场抓取（现场抓取很可能把进程拖死，
//     反而丢失现场）。
//
// 设计约束（对线上影响最小）：
//   - 使用独立的 http.ServeMux 与独立 listener，只绑回环地址（127.0.0.1）；
//     不进 Service / Ingress，公网与集群内其他 Pod 均不可达，
//     运维通过 ssh -L / kubectl port-forward 访问；
//   - 默认全部关闭，开启与否由 share.yml 的 debug 段决定；
//   - 任何子能力启动失败都只记日志，不阻断主服务。
//
// 安全提示：net/http/pprof 的 init() 会把 handler 注册到 http.DefaultServeMux，
// 这与是否使用空白导入无关（具名导入同样触发 init）。因此任何暴露 DefaultServeMux
// 的 HTTP server 都会顺带暴露 /debug/pprof/*。本仓库唯一这样的地方是
// internal/msggateway/ws_server.go（对外 WebSocket 端口），已改为独立 ServeMux。
// 后续新增 HTTP server 时务必沿用独立 ServeMux。
package debug

import (
	"context"
	"fmt"
	"net"
	"net/http"
	netpprof "net/http/pprof"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	runtimepprof "runtime/pprof"
	"sort"
	"strconv"
	"time"

	conf "github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/system/program"
	"github.com/openimsdk/tools/utils/datautil"
)

const (
	// defaultListenIP 只监听回环，公网与集群内其他 Pod 均不可达。
	defaultListenIP = "127.0.0.1"
	// defaultHeapDumpInterval 默认快照间隔。
	defaultHeapDumpInterval = 5 * time.Minute
	// defaultHeapDumpKeep 默认保留份数（每个服务/进程 5 分钟 × 6 = 最近约 30 分钟，组间互不挤占）。
	defaultHeapDumpKeep = 6
	// heapDumpPrefix / heapDumpSuffix 用于识别本包产生的快照文件。
	heapDumpPrefix = "heap-"
	heapDumpSuffix = ".pprof"
	// heapDumpDirName heapDump.dir 为空时的默认目录（建议部署时挂 emptyDir）。
	heapDumpDirName = "openim-pprof"
)

// Start 按配置启动诊断能力。cfg.Enable 为 false 时不做任何事。
//
// index 与服务的配置索引一致：实际 pprof 端口取 cfg.Ports[index]，
// 语义与 prometheus.ports 保持一致（一个进程可跑多个实例）。
func Start(ctx context.Context, cfg conf.Debug, index int) {
	if !cfg.Enable {
		return
	}

	ip := cfg.ListenIP
	if ip == "" {
		ip = defaultListenIP
	}

	switch port, err := datautil.GetElemByIndex(cfg.Ports, index); {
	case err != nil && len(cfg.Ports) > 0:
		// 配了端口但索引越界：这是配置错误，需要提示。
		log.ZWarn(ctx, "debug pprof disabled", err, "index", index, "ports", cfg.Ports)
	case err != nil:
		// 未配置端口：合法（例如只开 heap 快照不开 pprof 端口），静默跳过。
	case port > 0:
		go servePprof(ctx, ip, port)
	}

	if cfg.HeapDump.Enable {
		go dumpHeapLoop(ctx, cfg.HeapDump)
	}
}

// pprofMux 构造独立的 pprof 路由。
// 刻意不用 http.DefaultServeMux：避免影响既有 HTTP 路由。
// net/http/pprof 的 Index 会继续分发 /debug/pprof/{heap,goroutine,allocs,block,mutex,threadcreate}。
func pprofMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", netpprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", netpprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", netpprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", netpprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", netpprof.Trace)
	return mux
}

// servePprof 在独立端口上暴露 pprof。端口只绑回环地址，且使用独立 mux。
func servePprof(ctx context.Context, ip string, port int) {
	addr := net.JoinHostPort(ip, strconv.Itoa(port))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.ZWarn(ctx, "debug pprof listen failed", err, "addr", addr)
		return
	}

	srv := &http.Server{
		Handler:           pprofMux(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.ZInfo(ctx, "debug pprof server start", "addr", addr, "url", "http://"+addr+"/debug/pprof/")
	if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.ZWarn(ctx, "debug pprof server exit", err, "addr", addr)
	}
}

// dumpHeapLoop 按固定间隔写 heap 快照并轮转。
func dumpHeapLoop(ctx context.Context, cfg conf.HeapDump) {
	dir := cfg.Dir
	if dir == "" {
		dir = filepath.Join(os.TempDir(), heapDumpDirName)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.ZWarn(ctx, "heap dump disabled: create dir failed", err, "dir", dir)
		return
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultHeapDumpInterval
	}
	keep := cfg.Keep
	if keep <= 0 {
		keep = defaultHeapDumpKeep
	}

	log.ZInfo(ctx, "heap dump loop start",
		"dir", dir, "interval", interval.String(), "keep", keep, "gcBeforeDump", cfg.GCBeforeDump)

	// 启动后先等一个间隔再落第一份，避免与服务启动过程争抢资源。
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			log.ZDebug(ctx, "heap dump loop exit")
			return
		case <-timer.C:
			if err := dumpHeapOnce(ctx, dir, cfg.GCBeforeDump, keep); err != nil {
				log.ZWarn(ctx, "heap dump failed", err, "dir", dir)
			}
			timer.Reset(interval)
		}
	}
}

// dumpHeapOnce 写一份 heap profile 并保留最近 keep 份。
// 先写 .tmp 再 rename，保证目录里任何时刻都不存在半截文件。
func dumpHeapOnce(ctx context.Context, dir string, gcBefore bool, keep int) error {
	start := time.Now()
	if gcBefore {
		// 快照前强制一次 GC：让 inuse_space 反映真实存活对象，而不是待回收垃圾。
		// 这是单次、低频（默认 5 分钟一次）的成本，换来一个可直接归因的现场。
		runtime.GC()
	}

	// 文件名含 PID：同一服务的多实例是不同进程、PID 不同，由此每个实例分到独立分组，
	// "按分组各保留 keep 份"即变成"每个实例各保留 keep 份"——多实例服务也能保留完整时间维度历史。
	// 时间戳带毫秒：固定宽度，字典序即时间序，且同秒多次快照不会互相覆盖。
	name := fmt.Sprintf("%s%s-%d-%s%s",
		heapDumpPrefix, program.GetProcessName(), os.Getpid(), start.Format("20060102T150405.000"), heapDumpSuffix)
	finalPath := filepath.Join(dir, name)
	tmpPath := finalPath + ".tmp"

	written, err := writeHeapProfile(tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	rotate(dir, keep)
	log.ZDebug(ctx, "heap dump written",
		"file", finalPath, "size", written, "cost", time.Since(start).String())
	return nil
}

// writeHeapProfile 把当前 heap profile 写入 path，返回写入字节数。
func writeHeapProfile(path string) (int64, error) {
	profile := runtimepprof.Lookup("heap")
	if profile == nil {
		return 0, fmt.Errorf("heap profile not found")
	}
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	if err := profile.WriteTo(f, 0); err != nil {
		_ = f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// heapDumpNameRE 解析本包生成的快照文件名：heap-<进程名>-<时间戳>.pprof
// 进程名可能含连字符（如 openim-rpc-msg），时间戳位于文件名末尾、.pprof 之前，
// 因此用贪婪 (.+) 取到最后一个 '-' 之前作为"进程名（服务类型）"分组键。
var heapDumpNameRE = regexp.MustCompile(`^heap-(.+)-(.+)\.pprof$`)

// rotate 按"进程名（服务类型）"分组，每组各自只保留最近 keep 份，其余按文件名（时间序）删除。
// 文件名内嵌固定宽度时间戳，字典序即时间序；不同服务的快照互不挤占 keep 配额。
// 只处理本包生成的 heap-*.pprof，目录内其他文件（如 keepme.txt、半截 .tmp）不会被删除。
func rotate(dir string, keep int) {
	if keep <= 0 {
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	groups := make(map[string][]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		m := heapDumpNameRE.FindStringSubmatch(entry.Name())
		if m == nil {
			continue
		}
		groups[m[1]] = append(groups[m[1]], entry.Name())
	}
	for _, names := range groups {
		if len(names) <= keep {
			continue
		}
		sort.Strings(names)
		for _, name := range names[:len(names)-keep] {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
}
