package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// testEnv bundles an HTTP server with the store behind it, plus two knobs that
// let the same test body drive either backend.
type testEnv struct {
	srv        *httptest.Server
	st         Store
	setMaxReqs func(int)
	advance    func(time.Duration)
	mini       *miniredis.Miniredis // nil for the memory backend
}

func newTestEnv(t *testing.T, kind string, allowPrivate bool) testEnv {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	var env testEnv
	switch kind {
	case "memory":
		m := NewMemStore(DefaultTTL)
		env.st = m
		env.setMaxReqs = func(n int) { m.maxReqs = n }
		env.advance = func(d time.Duration) {
			base := m.now
			m.now = func() time.Time { return base().Add(d) }
		}
	case "redis":
		mr := miniredis.RunT(t)
		r, err := NewRedisStore(&redis.Options{Addr: mr.Addr()}, DefaultTTL, log)
		if err != nil {
			t.Fatalf("连接 miniredis 失败: %v", err)
		}
		t.Cleanup(func() { _ = r.Close() })
		env.st = r
		env.mini = mr
		env.setMaxReqs = func(n int) { r.maxReqs = n }
		env.advance = func(d time.Duration) {
			base := r.now
			r.now = func() time.Time { return base().Add(d) }
			mr.FastForward(d)
		}
	default:
		t.Fatalf("未知后端 %q", kind)
	}

	env.srv = httptest.NewServer(NewServer(env.st, log, allowPrivate).Handler())
	t.Cleanup(env.srv.Close)
	return env
}

// withBackends runs the same assertions against the production store (Redis)
// and the local fallback, so behavioural drift shows up immediately.
func withBackends(t *testing.T, allowPrivate bool, fn func(t *testing.T, e testEnv)) {
	t.Helper()
	for _, kind := range []string{"redis", "memory"} {
		t.Run(kind, func(t *testing.T) {
			fn(t, newTestEnv(t, kind, allowPrivate))
		})
	}
}

func createBin(t *testing.T, base string) string {
	t.Helper()
	resp, err := http.Post(base+"/api/sessions", "application/json", nil)
	if err != nil {
		t.Fatalf("创建接收箱: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("创建接收箱状态码 = %d，期望 201", resp.StatusCode)
	}
	var body struct {
		Token  string `json:"token"`
		BinURL string `json:"binUrl"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("解析创建响应: %v", err)
	}
	if len(body.Token) < 12 {
		t.Fatalf("token 太短: %q", body.Token)
	}
	if !strings.HasSuffix(body.BinURL, "/hook/"+body.Token) {
		t.Fatalf("binUrl %q 不以 /hook/%s 结尾", body.BinURL, body.Token)
	}
	return body.Token
}

func listRequests(t *testing.T, base, token string) ([]requestView, int) {
	t.Helper()
	resp, err := http.Get(base + "/api/sessions/" + token + "/requests")
	if err != nil {
		t.Fatalf("列出请求: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		Requests []requestView `json:"requests"`
		Dropped  int           `json:"dropped"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("解析列表: %v", err)
	}
	return body.Requests, body.Dropped
}

func TestCaptureRoundTrip(t *testing.T) {
	withBackends(t, false, func(t *testing.T, e testEnv) {
		token := createBin(t, e.srv.URL)

		payload := `{"event":"payment.succeeded","amount":4200}`
		req, _ := http.NewRequest(http.MethodPost,
			e.srv.URL+"/hook/"+token+"/stripe/evt_1?source=stripe&x=1",
			strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Stripe-Signature", "t=1,v1=abc")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("捕获请求: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("捕获状态码 = %d，期望 200", resp.StatusCode)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
			t.Fatalf("缺少 CORS 头，得到 %q", got)
		}

		reqs, _ := listRequests(t, e.srv.URL, token)
		if len(reqs) != 1 {
			t.Fatalf("得到 %d 条请求，期望 1", len(reqs))
		}
		got := reqs[0]
		if got.Method != http.MethodPost {
			t.Errorf("method = %q", got.Method)
		}
		if want := "/stripe/evt_1"; got.Path != want {
			t.Errorf("path = %q，期望 %q", got.Path, want)
		}
		if got.Query.Get("source") != "stripe" || got.Query.Get("x") != "1" {
			t.Errorf("query = %v", got.Query)
		}
		if got.Headers.Get("Stripe-Signature") != "t=1,v1=abc" {
			t.Errorf("自定义头未捕获: %v", got.Headers)
		}
		if got.Size != int64(len(payload)) {
			t.Errorf("size = %d，期望 %d", got.Size, len(payload))
		}
		if got.Preview == "" {
			t.Error("列表视图应带 preview")
		}
		if got.Body != "" {
			t.Errorf("列表视图不应带 body，得到 %q", got.Body)
		}

		dresp, err := http.Get(e.srv.URL + "/api/sessions/" + token + "/requests/" + got.ID)
		if err != nil {
			t.Fatalf("详情: %v", err)
		}
		defer dresp.Body.Close()
		var detail requestView
		if err := json.NewDecoder(dresp.Body).Decode(&detail); err != nil {
			t.Fatalf("解析详情: %v", err)
		}
		if detail.Body != payload {
			t.Errorf("body = %q，期望 %q", detail.Body, payload)
		}
		if detail.BodyEncoding != "" {
			t.Errorf("文本 body 不应 base64，得到 %q", detail.BodyEncoding)
		}
	})
}

func TestCaptureAllMethodsAndBinaryBody(t *testing.T) {
	withBackends(t, false, func(t *testing.T, e testEnv) {
		token := createBin(t, e.srv.URL)

		binary := []byte{0x00, 0x01, 0xff, 0xfe}
		for _, method := range []string{"GET", "PUT", "PATCH", "DELETE"} {
			req, _ := http.NewRequest(method, e.srv.URL+"/hook/"+token, bytes.NewReader(binary))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("%s: %v", method, err)
			}
			resp.Body.Close()
		}

		reqs, _ := listRequests(t, e.srv.URL, token)
		if len(reqs) != 4 {
			t.Fatalf("得到 %d 条请求，期望 4", len(reqs))
		}
		if reqs[0].Method != "DELETE" {
			t.Errorf("最新的 method = %q，期望 DELETE", reqs[0].Method)
		}
		if reqs[0].Path != "/" {
			t.Errorf("归一化 path = %q，期望 /", reqs[0].Path)
		}

		dresp, err := http.Get(e.srv.URL + "/api/sessions/" + token + "/requests/" + reqs[0].ID)
		if err != nil {
			t.Fatalf("详情: %v", err)
		}
		defer dresp.Body.Close()
		var detail requestView
		_ = json.NewDecoder(dresp.Body).Decode(&detail)
		if detail.BodyEncoding != "base64" {
			t.Fatalf("二进制 body 编码 = %q，期望 base64", detail.BodyEncoding)
		}
	})
}

func TestOptionsIsNotRecorded(t *testing.T) {
	withBackends(t, false, func(t *testing.T, e testEnv) {
		token := createBin(t, e.srv.URL)

		req, _ := http.NewRequest(http.MethodOptions, e.srv.URL+"/hook/"+token, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("OPTIONS: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("OPTIONS 状态码 = %d，期望 204", resp.StatusCode)
		}
		if reqs, _ := listRequests(t, e.srv.URL, token); len(reqs) != 0 {
			t.Fatalf("预检不应被记录，得到 %d 条", len(reqs))
		}
	})
}

func TestBodyTruncation(t *testing.T) {
	withBackends(t, false, func(t *testing.T, e testEnv) {
		token := createBin(t, e.srv.URL)

		big := bytes.Repeat([]byte("a"), MaxBodyBytes+512)
		resp, err := http.Post(e.srv.URL+"/hook/"+token, "text/plain", bytes.NewReader(big))
		if err != nil {
			t.Fatalf("捕获: %v", err)
		}
		resp.Body.Close()

		reqs, _ := listRequests(t, e.srv.URL, token)
		if len(reqs) != 1 || !reqs[0].Truncated {
			t.Fatalf("期望一条被截断的请求，得到 %+v", reqs)
		}
		dresp, err := http.Get(e.srv.URL + "/api/sessions/" + token + "/requests/" + reqs[0].ID)
		if err != nil {
			t.Fatalf("详情: %v", err)
		}
		defer dresp.Body.Close()
		var detail requestView
		_ = json.NewDecoder(dresp.Body).Decode(&detail)
		if len(detail.Body) != MaxBodyBytes {
			t.Fatalf("存储的 body = %d 字节，期望 %d", len(detail.Body), MaxBodyBytes)
		}
	})
}

func TestSessionDropsOldestBeyondCap(t *testing.T) {
	withBackends(t, false, func(t *testing.T, e testEnv) {
		token := createBin(t, e.srv.URL)
		e.setMaxReqs(3)

		for i := 0; i < 5; i++ {
			resp, err := http.Get(e.srv.URL + "/hook/" + token)
			if err != nil {
				t.Fatalf("捕获 %d: %v", i, err)
			}
			resp.Body.Close()
		}

		reqs, dropped := listRequests(t, e.srv.URL, token)
		if len(reqs) != 3 {
			t.Fatalf("保留 %d 条请求，期望 3", len(reqs))
		}
		if dropped != 2 {
			t.Fatalf("dropped = %d，期望 2", dropped)
		}
	})
}

func TestExpiredSessionRejectsCapture(t *testing.T) {
	withBackends(t, false, func(t *testing.T, e testEnv) {
		token := createBin(t, e.srv.URL)
		e.advance(DefaultTTL + time.Minute)

		resp, err := http.Get(e.srv.URL + "/hook/" + token)
		if err != nil {
			t.Fatalf("捕获: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("过期后捕获状态码 = %d，期望 404", resp.StatusCode)
		}
		if _, err := e.st.Get(token); err == nil {
			t.Fatal("过期接收箱应已不存在")
		}
	})
}

func TestUnknownBinIs404(t *testing.T) {
	withBackends(t, false, func(t *testing.T, e testEnv) {
		resp, err := http.Get(e.srv.URL + "/hook/doesnotexist")
		if err != nil {
			t.Fatalf("捕获: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("状态码 = %d，期望 404", resp.StatusCode)
		}
	})
}

func TestSSEStreamsNewRequest(t *testing.T) {
	withBackends(t, false, func(t *testing.T, e testEnv) {
		token := createBin(t, e.srv.URL)

		resp, err := http.Get(e.srv.URL + "/api/sessions/" + token + "/events")
		if err != nil {
			t.Fatalf("事件流: %v", err)
		}
		defer resp.Body.Close()
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
			t.Fatalf("content-type = %q", ct)
		}

		lines := make(chan string, 64)
		go func() {
			sc := bufio.NewScanner(resp.Body)
			for sc.Scan() {
				lines <- sc.Text()
			}
			close(lines)
		}()

		// ": connected" 在订阅注册之后写出，看到它就说明捕获会被广播。
		waitFor(t, lines, ": connected", 3*time.Second)

		go func() {
			r, err := http.Post(e.srv.URL+"/hook/"+token, "application/json", strings.NewReader(`{"a":1}`))
			if err == nil {
				r.Body.Close()
			}
		}()

		waitFor(t, lines, "event: request", 5*time.Second)
	})
}

func waitFor(t *testing.T, lines <-chan string, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("等待 %q 时流已关闭", want)
			}
			if strings.Contains(line, want) {
				return
			}
		case <-deadline:
			t.Fatalf("等待 %q 超时", want)
		}
	}
}

func TestReplayBlocksPrivateAddresses(t *testing.T) {
	withBackends(t, false, func(t *testing.T, e testEnv) {
		token := createBin(t, e.srv.URL)

		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("不应能访问内网目标")
		}))
		defer target.Close()

		resp, err := http.Post(e.srv.URL+"/hook/"+token, "application/json", strings.NewReader(`{"a":1}`))
		if err != nil {
			t.Fatalf("捕获: %v", err)
		}
		resp.Body.Close()
		reqs, _ := listRequests(t, e.srv.URL, token)

		body, _ := json.Marshal(map[string]string{"targetUrl": target.URL})
		rresp, err := http.Post(
			e.srv.URL+"/api/sessions/"+token+"/replay?id="+reqs[0].ID,
			"application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("回放: %v", err)
		}
		defer rresp.Body.Close()
		if rresp.StatusCode != http.StatusBadGateway {
			t.Fatalf("回放到内网地址状态码 = %d，期望 502", rresp.StatusCode)
		}
	})
}

func TestReplayForwardsRequest(t *testing.T) {
	withBackends(t, true, func(t *testing.T, e testEnv) {
		token := createBin(t, e.srv.URL)

		type seen struct {
			method, path, body, sig string
		}
		got := make(chan seen, 1)
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			got <- seen{r.Method, r.URL.Path, string(b), r.Header.Get("X-Custom")}
			w.WriteHeader(http.StatusTeapot)
			_, _ = w.Write([]byte(`replayed`))
		}))
		defer target.Close()

		req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/hook/"+token+"/orders", strings.NewReader(`{"id":7}`))
		req.Header.Set("X-Custom", "yes")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("捕获: %v", err)
		}
		resp.Body.Close()
		reqs, _ := listRequests(t, e.srv.URL, token)

		body, _ := json.Marshal(map[string]string{"targetUrl": target.URL + "/orders"})
		rresp, err := http.Post(
			e.srv.URL+"/api/sessions/"+token+"/replay?id="+reqs[0].ID,
			"application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("回放: %v", err)
		}
		defer rresp.Body.Close()
		if rresp.StatusCode != http.StatusOK {
			raw, _ := io.ReadAll(rresp.Body)
			t.Fatalf("回放状态码 = %d，body = %s", rresp.StatusCode, raw)
		}

		var out replayOutput
		if err := json.NewDecoder(rresp.Body).Decode(&out); err != nil {
			t.Fatalf("解析回放结果: %v", err)
		}
		if out.Status != http.StatusTeapot {
			t.Errorf("回放目标返回 = %d，期望 418", out.Status)
		}
		if out.Body != "replayed" {
			t.Errorf("回放目标 body = %q", out.Body)
		}

		select {
		case s := <-got:
			if s.method != http.MethodPost || s.path != "/orders" || s.body != `{"id":7}` || s.sig != "yes" {
				t.Errorf("目标服务收到 %+v", s)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("目标服务始终没收到回放请求")
		}
	})
}

func TestReplayRejectsBadTarget(t *testing.T) {
	withBackends(t, true, func(t *testing.T, e testEnv) {
		token := createBin(t, e.srv.URL)
		resp, err := http.Get(e.srv.URL + "/hook/" + token)
		if err != nil {
			t.Fatalf("捕获: %v", err)
		}
		resp.Body.Close()
		reqs, _ := listRequests(t, e.srv.URL, token)

		for _, bad := range []string{"", "ftp://example.com", "file:///etc/passwd", "not a url"} {
			body, _ := json.Marshal(map[string]string{"targetUrl": bad})
			rresp, err := http.Post(
				e.srv.URL+"/api/sessions/"+token+"/replay?id="+reqs[0].ID,
				"application/json", bytes.NewReader(body))
			if err != nil {
				t.Fatalf("回放 %q: %v", bad, err)
			}
			rresp.Body.Close()
			if rresp.StatusCode != http.StatusBadRequest {
				t.Errorf("目标 %q 状态码 = %d，期望 400", bad, rresp.StatusCode)
			}
		}
	})
}

func TestHealthz(t *testing.T) {
	withBackends(t, false, func(t *testing.T, e testEnv) {
		resp, err := http.Get(e.srv.URL + "/healthz")
		if err != nil {
			t.Fatalf("healthz: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("healthz 状态码 = %d", resp.StatusCode)
		}
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("解析 healthz: %v", err)
		}
		if body["storage"] != e.st.Name() {
			t.Errorf("storage = %v，期望 %q", body["storage"], e.st.Name())
		}
	})
}

func TestCreateSessionRateLimited(t *testing.T) {
	withBackends(t, false, func(t *testing.T, e testEnv) {
		for i := 0; i < 10; i++ {
			createBin(t, e.srv.URL)
		}
		resp, err := http.Post(e.srv.URL+"/api/sessions", "application/json", nil)
		if err != nil {
			t.Fatalf("创建: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("第 11 次创建状态码 = %d，期望 429", resp.StatusCode)
		}
		if resp.Header.Get("Retry-After") == "" {
			t.Error("缺少 Retry-After 头")
		}
	})
}

// --- Redis 专属行为 -------------------------------------------------------

func TestRedisKeysHaveTTL(t *testing.T) {
	e := newTestEnv(t, "redis", false)
	token := createBin(t, e.srv.URL)

	if ttl := e.mini.TTL(metaKey(token)); ttl <= 0 {
		t.Fatalf("%s 未设置 TTL", metaKey(token))
	} else if ttl > DefaultTTL+time.Minute || ttl < DefaultTTL-time.Minute {
		t.Errorf("meta TTL = %v，期望约 %v", ttl, DefaultTTL)
	}

	resp, err := http.Post(e.srv.URL+"/hook/"+token, "text/plain", strings.NewReader("hi"))
	if err != nil {
		t.Fatalf("捕获: %v", err)
	}
	resp.Body.Close()
	for _, key := range []string{idsKey(token), dataKey(token)} {
		if ttl := e.mini.TTL(key); ttl <= 0 {
			t.Errorf("%s 未设置 TTL", key)
		}
	}
}

func TestRedisEvictsDeletedPayloads(t *testing.T) {
	e := newTestEnv(t, "redis", false)
	token := createBin(t, e.srv.URL)
	e.setMaxReqs(2)

	// 3 次捕获，第 1 条应被淘汰，data hash 里只留 2 条，避免残留耗内存。
	for i := 0; i < 3; i++ {
		resp, err := http.Post(e.srv.URL+"/hook/"+token, "text/plain", strings.NewReader("x"))
		if err != nil {
			t.Fatalf("捕获: %v", err)
		}
		resp.Body.Close()
	}

	hkeys, err := e.mini.HKeys(dataKey(token))
	if err != nil {
		t.Fatalf("读取 data hash 失败: %v", err)
	}
	if len(hkeys) != 2 {
		t.Errorf("data hash 有 %d 条，期望 2", len(hkeys))
	}
	idList, err := e.mini.List(idsKey(token))
	if err != nil {
		t.Fatalf("读取 ids list 失败: %v", err)
	}
	if len(idList) != 2 {
		t.Errorf("ids list 有 %d 条，期望 2", len(idList))
	}
	_, dropped := listRequests(t, e.srv.URL, token)
	if dropped != 1 {
		t.Errorf("dropped = %d，期望 1", dropped)
	}
}

// 两个独立的 RedisStore 代表两个 app 实例：A 收到请求，B 上的 SSE 订阅者
// 必须也能收到事件，并且 B 能读到同一份数据。这是换 Redis 的核心收益。
func TestRedisPubSubAcrossInstances(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mr := miniredis.RunT(t)

	a, err := NewRedisStore(&redis.Options{Addr: mr.Addr()}, DefaultTTL, log)
	if err != nil {
		t.Fatalf("实例 A 连接失败: %v", err)
	}
	defer a.Close()
	b, err := NewRedisStore(&redis.Options{Addr: mr.Addr()}, DefaultTTL, log)
	if err != nil {
		t.Fatalf("实例 B 连接失败: %v", err)
	}
	defer b.Close()

	sess, err := a.Create()
	if err != nil {
		t.Fatalf("创建接收箱: %v", err)
	}

	ch, cancel, err := b.Subscribe(sess.Token)
	if err != nil {
		t.Fatalf("实例 B 订阅失败: %v", err)
	}
	defer cancel()

	cap := &Captured{
		ID:            randomToken(9),
		Method:        http.MethodPost,
		Path:          "/from-a",
		Header:        http.Header{"X-Test": []string{"yes"}},
		Body:          []byte(`{"hello":"world"}`),
		ContentLength: 17,
		RemoteAddr:    "203.0.113.7",
		ReceivedAt:    time.Now().UTC(),
	}
	if err := a.Add(sess.Token, cap); err != nil {
		t.Fatalf("实例 A 写入失败: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.ID != cap.ID || ev.Path != "/from-a" || ev.Preview == "" {
			t.Errorf("实例 B 收到 %+v", ev)
		}
		raw, _ := json.Marshal(ev)
		if bytes.Contains(raw, []byte(`"body"`)) {
			t.Error("事件不应携带 body 内容，避免 1 MiB 广播")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("实例 B 的 SSE 订阅者没收到跨实例事件")
	}

	// B 直接读 Redis 也能看到 A 写入的请求。
	reqs, _, err := b.List(sess.Token)
	if err != nil {
		t.Fatalf("实例 B 读取失败: %v", err)
	}
	if len(reqs) != 1 || string(reqs[0].Body) != `{"hello":"world"}` {
		t.Fatalf("实例 B 读到的数据不对: %+v", reqs)
	}
}

func TestStoredBodyRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"text", []byte(`{"a":1}`)},
		{"utf8", []byte("你好，世界")},
		{"binary", []byte{0x00, 0x01, 0xff, 0xfe, 0x7f}},
		{"empty", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orig := &Captured{
				ID: "id", Method: "POST", Path: "/p",
				Query:         map[string][]string{"q": {"1"}},
				Header:        http.Header{"X-A": []string{"b"}},
				Body:          tc.body,
				ContentLength: int64(len(tc.body)),
				RemoteAddr:    "1.2.3.4",
				ReceivedAt:    time.Now().UTC(),
			}
			b, err := encodeCaptured(orig)
			if err != nil {
				t.Fatalf("编码: %v", err)
			}
			got, err := decodeCaptured(b)
			if err != nil {
				t.Fatalf("解码: %v", err)
			}
			if !bytes.Equal(got.Body, tc.body) {
				t.Errorf("body 往返不一致: %v != %v", got.Body, tc.body)
			}
			if got.Query.Get("q") != "1" || got.Header.Get("X-A") != "b" {
				t.Error("query/header 丢失")
			}
			if got.ID != orig.ID || got.ContentLength != orig.ContentLength {
				t.Error("元信息丢失")
			}
		})
	}
}
