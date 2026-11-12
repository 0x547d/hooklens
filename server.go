package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	_ "embed"
)

//go:embed web/index.html
var indexHTML []byte

const (
	maxReplayBody     = 64 << 10
	replayTimeout     = 15 * time.Second
	replayMaxRedirect = 3
	sseHeartbeat      = 25 * time.Second
)

// Server wires the HTTP surface to the store.
type Server struct {
	store        Store
	log          *slog.Logger
	allowPrivate bool
	replay       *http.Client
	createLimit  *ipLimiter
}

func NewServer(store Store, log *slog.Logger, allowPrivate bool) *Server {
	return &Server{
		store:        store,
		log:          log,
		allowPrivate: allowPrivate,
		replay:       newReplayClient(allowPrivate),
		createLimit:  newIPLimiter(10, time.Hour),
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", s.handleApp)
	mux.HandleFunc("GET /s/{token}", s.handleApp)
	mux.HandleFunc("GET /healthz", s.handleHealth)

	mux.HandleFunc("POST /api/sessions", s.handleCreateSession)
	mux.HandleFunc("GET /api/sessions/{token}", s.handleSessionMeta)
	mux.HandleFunc("GET /api/sessions/{token}/requests", s.handleList)
	mux.HandleFunc("GET /api/sessions/{token}/requests/{id}", s.handleRequestDetail)
	mux.HandleFunc("GET /api/sessions/{token}/events", s.handleEvents)
	mux.HandleFunc("POST /api/sessions/{token}/replay", s.handleReplay)

	// Capture endpoint: any method, any depth of trailing path.
	mux.HandleFunc("/hook/{token}", s.handleCapture)
	mux.HandleFunc("/hook/{token}/{rest...}", s.handleCapture)

	return s.withRecovery(mux)
}

func (s *Server) withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				s.log.Error("panic serving request", "path", r.URL.Path, "recover", v)
				http.Error(w, "服务器内部错误", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// --- pages ---------------------------------------------------------------

func (s *Server) handleApp(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(indexHTML)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"storage": s.store.Name(),
		"bins":    s.store.Count(),
	})
}

// --- session API ---------------------------------------------------------

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	if !s.createLimit.allow(clientIP(r)) {
		w.Header().Set("Retry-After", "600")
		writeErr(w, http.StatusTooManyRequests, "创建太频繁了，请稍后再试")
		return
	}
	sess, err := s.store.Create()
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	base := baseURL(r)
	writeJSON(w, http.StatusCreated, map[string]any{
		"token":     sess.Token,
		"binUrl":    base + "/hook/" + sess.Token,
		"pageUrl":   base + "/s/" + sess.Token,
		"createdAt": sess.Created,
		"expiresAt": sess.Expires,
	})
}

func (s *Server) handleSessionMeta(w http.ResponseWriter, r *http.Request) {
	sess, err := s.store.Get(r.PathValue("token"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "接收箱不存在或已过期")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"token":     sess.Token,
		"binUrl":    baseURL(r) + "/hook/" + sess.Token,
		"pageUrl":   baseURL(r) + "/s/" + sess.Token,
		"createdAt": sess.Created,
		"expiresAt": sess.Expires,
	})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")
	reqs, dropped, err := s.store.List(token)
	if err != nil {
		writeErr(w, http.StatusNotFound, "接收箱不存在或已过期")
		return
	}
	out := make([]requestView, 0, len(reqs))
	for _, c := range reqs {
		out = append(out, toView(c, false))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"requests": out,
		"dropped":  dropped,
	})
}

func (s *Server) handleRequestDetail(w http.ResponseWriter, r *http.Request) {
	c, err := s.store.ByID(r.PathValue("token"), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "请求不存在")
		return
	}
	writeJSON(w, http.StatusOK, toView(c, true))
}

// --- capture -------------------------------------------------------------

func (s *Server) handleCapture(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")

	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	body, truncated, err := readLimited(r.Body, MaxBodyBytes)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无法读取请求体")
		return
	}

	rest := strings.TrimPrefix(r.URL.Path, "/hook/"+token)
	if rest == "" {
		rest = "/"
	}

	c := &Captured{
		ID:            randomToken(9),
		Method:        r.Method,
		Path:          rest,
		Query:         r.URL.Query(),
		Header:        r.Header.Clone(),
		Body:          body,
		Truncated:     truncated,
		ContentLength: r.ContentLength,
		RemoteAddr:    clientIP(r),
		ReceivedAt:    time.Now().UTC(),
	}
	if c.ContentLength < 0 {
		c.ContentLength = int64(len(body))
	}

	if err := s.store.Add(token, c); err != nil {
		writeErr(w, http.StatusNotFound, "接收箱不存在或已过期")
		return
	}

	s.log.Info("captured", "bin", token, "method", c.Method, "path", c.Path, "bytes", len(body))
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"id":         c.ID,
		"receivedAt": c.ReceivedAt,
	})
}

// --- live stream ---------------------------------------------------------

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")

	ch, cancel, err := s.store.Subscribe(token)
	if err != nil {
		writeErr(w, http.StatusNotFound, "接收箱不存在或已过期")
		return
	}
	defer cancel()

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "当前环境不支持流式响应")
		return
	}

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")

	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	ticker := time.NewTicker(sseHeartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev, open := <-ch:
			if !open {
				return
			}
			payload, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: request\ndata: %s\n\n", payload)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

// --- replay --------------------------------------------------------------

type replayInput struct {
	TargetURL string `json:"targetUrl"`
}

type replayOutput struct {
	Status     int         `json:"status"`
	DurationMs int64       `json:"durationMs"`
	Headers    http.Header `json:"headers"`
	Body       string      `json:"body"`
	Encoding   string      `json:"encoding,omitempty"`
	Truncated  bool        `json:"truncated"`
}

func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")

	var in replayInput
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<10)).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "请求格式不正确")
		return
	}
	target, err := normalizeTarget(in.TargetURL)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	src, err := s.store.ByID(token, r.URL.Query().Get("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "请求不存在")
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), src.Method, target.String(), strings.NewReader(string(src.Body)))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "无法构造重放请求")
		return
	}
	for k, vals := range src.Header {
		if isHopByHop(k) {
			continue
		}
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("X-Hooklens-Replay", "1")

	start := time.Now()
	resp, err := s.replay.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "重放失败："+err.Error())
		return
	}
	defer resp.Body.Close()

	body, truncated, _ := readLimited(resp.Body, maxReplayBody)
	out := replayOutput{
		Status:     resp.StatusCode,
		DurationMs: elapsed.Milliseconds(),
		Headers:    resp.Header,
		Body:       string(body),
		Truncated:  truncated,
	}
	if !utf8.Valid(body) {
		out.Body = base64.StdEncoding.EncodeToString(body)
		out.Encoding = "base64"
	}
	writeJSON(w, http.StatusOK, out)
}

// --- helpers -------------------------------------------------------------

type requestView struct {
	ID           string      `json:"id"`
	Method       string      `json:"method"`
	Path         string      `json:"path"`
	Query        url.Values  `json:"query"`
	Headers      http.Header `json:"headers"`
	Preview      string      `json:"preview"`
	Size         int64       `json:"size"`
	Truncated    bool        `json:"truncated"`
	RemoteAddr   string      `json:"remoteAddr"`
	ReceivedAt   time.Time   `json:"receivedAt"`
	Body         string      `json:"body,omitempty"`
	BodyEncoding string      `json:"bodyEncoding,omitempty"`
}

func toView(c *Captured, withBody bool) requestView {
	v := requestView{
		ID:         c.ID,
		Method:     c.Method,
		Path:       c.Path,
		Query:      c.Query,
		Headers:    c.Header,
		Preview:    c.Preview(160),
		Size:       c.ContentLength,
		Truncated:  c.Truncated,
		RemoteAddr: c.RemoteAddr,
		ReceivedAt: c.ReceivedAt,
	}
	if withBody {
		if c.IsText() {
			v.Body = string(c.Body)
		} else {
			v.Body = base64.StdEncoding.EncodeToString(c.Body)
			v.BodyEncoding = "base64"
		}
	}
	return v
}

func normalizeTarget(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("请填写转发目标地址")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("目标地址格式不正确")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.New("目标地址必须以 http:// 或 https:// 开头")
	}
	if u.Host == "" {
		return nil, errors.New("目标地址缺少主机名")
	}
	return u, nil
}

func readLimited(r io.Reader, limit int64) ([]byte, bool, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(b)) > limit {
		return b[:limit], true, nil
	}
	return b, false, nil
}

func isHopByHop(h string) bool {
	switch strings.ToLower(h) {
	case "host", "content-length", "connection", "transfer-encoding",
		"keep-alive", "proxy-authorization", "proxy-connection",
		"te", "trailer", "upgrade":
		return true
	}
	return false
}

func clientIP(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

func baseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if p := r.Header.Get("X-Forwarded-Proto"); p != "" {
		scheme = strings.TrimSpace(strings.Split(p, ",")[0])
	}
	host := r.Host
	if h := r.Header.Get("X-Forwarded-Host"); h != "" {
		host = strings.TrimSpace(strings.Split(h, ",")[0])
	}
	return scheme + "://" + host
}

func setCORS(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "*")
	h.Set("Access-Control-Max-Age", "86400")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// --- outbound replay client ---------------------------------------------

// newReplayClient returns a client that refuses to connect to loopback,
// private, link-local or multicast addresses. Without this, the replay
// endpoint is an open proxy into whatever network the server sits in —
// including cloud metadata endpoints.
func newReplayClient(allowPrivate bool) *http.Client {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	if !allowPrivate {
		dialer.Control = func(network, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip := net.ParseIP(host)
			if ip == nil {
				return fmt.Errorf("拒绝连接 %q", host)
			}
			if blockedIP(ip) {
				return fmt.Errorf("拒绝连接非公网地址 %s", ip)
			}
			return nil
		}
	}
	return &http.Client{
		Timeout: replayTimeout,
		Transport: &http.Transport{
			DialContext:           dialer.DialContext,
			MaxIdleConns:          10,
			IdleConnTimeout:       30 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: 10 * time.Second,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= replayMaxRedirect {
				return errors.New("重定向次数过多")
			}
			return nil
		},
	}
}

func blockedIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() {
		return true
	}
	// 100.64.0.0/10 carrier-grade NAT, used by some cloud metadata proxies.
	if v4 := ip.To4(); v4 != nil && v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
		return true
	}
	return false
}

// --- abuse control -------------------------------------------------------

// ipLimiter is a fixed-window counter that stops one client from minting bins
// without bound. Crude, but the failure mode is a short wait, not data loss.
type ipLimiter struct {
	mu     sync.Mutex
	hits   map[string]*limitWindow
	limit  int
	window time.Duration
	now    func() time.Time
}

type limitWindow struct {
	count int
	reset time.Time
}

func newIPLimiter(limit int, window time.Duration) *ipLimiter {
	return &ipLimiter{
		hits:   make(map[string]*limitWindow),
		limit:  limit,
		window: window,
		now:    time.Now,
	}
}

func (l *ipLimiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	if len(l.hits) > 10000 {
		for k, w := range l.hits {
			if !now.Before(w.reset) {
				delete(l.hits, k)
			}
		}
	}

	w, ok := l.hits[key]
	if !ok || !now.Before(w.reset) {
		l.hits[key] = &limitWindow{count: 1, reset: now.Add(l.window)}
		return true
	}
	if w.count >= l.limit {
		return false
	}
	w.count++
	return true
}
