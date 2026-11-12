package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRealRedisIntegration 是可选测试：只有设置了 HOOKLENS_REDIS_TEST_ADDR
// 才会运行。miniredis 是模拟器，Lua 脚本、PEXPIREAT 和 PSUBSCRIBE 必须在
// 真实 Redis 上验一遍才算数。
//
//	HOOKLENS_REDIS_TEST_ADDR=127.0.0.1:6379 go test -race -run TestRealRedisIntegration -v
//
// 固定使用 DB 15 并在开始时 FLUSHDB，不会碰到其他数据。
func TestRealRedisIntegration(t *testing.T) {
	addr := os.Getenv("HOOKLENS_REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("未设置 HOOKLENS_REDIS_TEST_ADDR，跳过真实 Redis 集成测试")
	}

	opts := &redis.Options{Addr: addr, DB: 15}
	probe := redis.NewClient(opts)
	ctx := context.Background()
	if err := probe.Ping(ctx).Err(); err != nil {
		t.Fatalf("连接真实 Redis 失败: %v", err)
	}
	if err := probe.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("清空 DB 15 失败: %v", err)
	}
	defer func() { _ = probe.FlushDB(ctx); _ = probe.Close() }()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// --- 1. 走完整 HTTP 链路，证明真实 Redis 上的读写与顺序 ---
	st, err := NewRedisStore(opts, 10*time.Minute, log)
	if err != nil {
		t.Fatalf("创建 RedisStore 失败: %v", err)
	}
	defer st.Close()

	srv := httptest.NewServer(NewServer(st, log, false).Handler())
	defer srv.Close()

	token := createBin(t, srv.URL)
	for _, p := range []string{"/first", "/second", "/third"} {
		resp, err := http.Post(srv.URL+"/hook/"+token+p, "application/json",
			strings.NewReader(`{"path":"`+p+`"}`))
		if err != nil {
			t.Fatalf("捕获 %s: %v", p, err)
		}
		resp.Body.Close()
	}

	reqs, _ := listRequests(t, srv.URL, token)
	if len(reqs) != 3 {
		t.Fatalf("得到 %d 条请求，期望 3", len(reqs))
	}
	if reqs[0].Path != "/third" || reqs[2].Path != "/first" {
		t.Fatalf("顺序不对: %s, %s, %s", reqs[0].Path, reqs[1].Path, reqs[2].Path)
	}
	var detail requestView
	dresp, err := http.Get(srv.URL + "/api/sessions/" + token + "/requests/" + reqs[0].ID)
	if err != nil {
		t.Fatalf("详情: %v", err)
	}
	if err := json.NewDecoder(dresp.Body).Decode(&detail); err != nil {
		t.Fatalf("解析详情: %v", err)
	}
	dresp.Body.Close()
	if detail.Body != `{"path":"/third"}` {
		t.Errorf("详情 body = %q", detail.Body)
	}

	// --- 2. 三个 key 都真的带 TTL ---
	for _, key := range []string{metaKey(token), idsKey(token), dataKey(token)} {
		ttl, err := probe.PTTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("PTTL %s: %v", key, err)
		}
		if ttl <= 0 || ttl > 10*time.Minute {
			t.Errorf("%s TTL = %v，期望接近 10 分钟", key, ttl)
		}
	}

	// --- 3. 超限淘汰：Lua 必须同时删掉 data hash 里的孤儿字段 ---
	st.maxReqs = 2
	resp, err := http.Post(srv.URL+"/hook/"+token+"/fourth", "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatalf("第 4 次捕获: %v", err)
	}
	resp.Body.Close()

	if n := probe.HLen(ctx, dataKey(token)).Val(); n != 2 {
		t.Errorf("data hash 有 %d 个字段，期望 2", n)
	}
	if n := probe.LLen(ctx, idsKey(token)).Val(); n != 2 {
		t.Errorf("ids list 有 %d 个元素，期望 2", n)
	}
	_, dropped := listRequests(t, srv.URL, token)
	if dropped != 2 {
		t.Errorf("dropped = %d，期望 2", dropped)
	}

	// --- 4. 跨实例 Pub/Sub（真实 Redis 的 PSUBSCRIBE）---
	other, err := NewRedisStore(opts, 10*time.Minute, log)
	if err != nil {
		t.Fatalf("创建第二个实例失败: %v", err)
	}
	defer other.Close()

	ch, cancel, err := other.Subscribe(token)
	if err != nil {
		t.Fatalf("订阅失败: %v", err)
	}
	defer cancel()

	go func() {
		r, err := http.Post(srv.URL+"/hook/"+token+"/broadcast", "text/plain", strings.NewReader("hi"))
		if err == nil {
			r.Body.Close()
		}
	}()
	select {
	case ev := <-ch:
		if ev.Path != "/broadcast" {
			t.Errorf("收到 %q，期望 /broadcast", ev.Path)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("真实 Redis 的 Pub/Sub 没有把事件送到另一个实例")
	}

	// --- 5. 过期完全由 Redis 负责，进程不参与 ---
	short, err := NewRedisStore(opts, 1200*time.Millisecond, log)
	if err != nil {
		t.Fatalf("创建短 TTL 实例失败: %v", err)
	}
	defer short.Close()

	sess, err := short.Create()
	if err != nil {
		t.Fatalf("创建短 TTL 接收箱: %v", err)
	}
	if err := short.Add(sess.Token, &Captured{
		ID: "will-expire", Method: "GET", Path: "/", Body: []byte("x"),
		ContentLength: 1, ReceivedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("写入短 TTL 接收箱: %v", err)
	}
	time.Sleep(1600 * time.Millisecond)

	if _, err := short.Get(sess.Token); err == nil {
		t.Error("Redis 过期后接收箱仍然可读")
	}
	for _, key := range []string{metaKey(sess.Token), idsKey(sess.Token), dataKey(sess.Token)} {
		if probe.Exists(ctx, key).Val() != 0 {
			t.Errorf("Redis 过期后 %s 仍存在", key)
		}
	}
}
