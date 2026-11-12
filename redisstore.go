package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	keyPrefix    = "hooklens:"
	binsKey      = keyPrefix + "bins"    // ZSET token -> expiresAt(ms), for Count / cap
	eventsPrefix = keyPrefix + "events:" // Pub/Sub channel prefix
)

func metaKey(token string) string       { return keyPrefix + "bin:" + token } // HASH: 元信息 + dropped
func idsKey(token string) string        { return metaKey(token) + ":ids" }    // LIST: 请求 ID，新的在前
func dataKey(token string) string       { return metaKey(token) + ":data" }   // HASH: id -> 请求 JSON
func eventsChannel(token string) string { return eventsPrefix + token }

// createScript enforces the global bin cap and writes bin metadata atomically.
// Scores are unix-ms so PEXPIREAT and ZSET expiry stay in agreement.
var createScript = redis.NewScript(`
local binsKey   = KEYS[1]
local metaKey   = KEYS[2]
local token     = ARGV[1]
local createdAt = ARGV[2]
local expiresMs = ARGV[3]
local nowMs     = ARGV[4]
local maxBins   = tonumber(ARGV[5])

redis.call('ZREMRANGEBYSCORE', binsKey, '-inf', nowMs)
if redis.call('ZCARD', binsKey) >= maxBins then
  return -1
end
if redis.call('EXISTS', metaKey) == 1 then
  return 0
end
redis.call('HSET', metaKey, 'token', token, 'createdAt', createdAt, 'expiresAt', expiresMs, 'dropped', 0)
redis.call('PEXPIREAT', metaKey, expiresMs)
redis.call('ZADD', binsKey, expiresMs, token)
return 1
`)

// addScript appends a request, trims to the per-bin cap, removes the evicted
// payloads from the data hash, and refreshes TTLs — all in one round trip.
// Returning 0 means the bin disappeared between Get and the write.
var addScript = redis.NewScript(`
local idsKey    = KEYS[1]
local dataKey   = KEYS[2]
local metaKey   = KEYS[3]
local id        = ARGV[1]
local payload   = ARGV[2]
local max       = tonumber(ARGV[3])
local expiresMs = ARGV[4]

if redis.call('EXISTS', metaKey) == 0 then
  return 0
end
redis.call('LPUSH', idsKey, id)
redis.call('HSET', dataKey, id, payload)
local n = redis.call('LLEN', idsKey)
if n > max then
  local extra = redis.call('LRANGE', idsKey, max, -1)
  for _, rid in ipairs(extra) do
    redis.call('HDEL', dataKey, rid)
  end
  redis.call('LTRIM', idsKey, 0, max - 1)
  redis.call('HINCRBY', metaKey, 'dropped', n - max)
  n = max
end
redis.call('PEXPIREAT', idsKey, expiresMs)
redis.call('PEXPIREAT', dataKey, expiresMs)
return n
`)

// RedisStore is the production store. All state lives in Redis, so any number
// of app instances can serve the same bin and SSE still works: captures are
// published on a channel and every instance fans them out to its own clients.
type RedisStore struct {
	rdb     *redis.Client
	ttl     time.Duration
	now     func() time.Time
	maxReqs int
	maxBins int
	log     *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	ps     *redis.PubSub
	wg     sync.WaitGroup

	mu   sync.Mutex
	subs map[string]map[chan *RequestEvent]struct{}
}

// NewRedisStore owns the client it creates. Call Close on shutdown.
func NewRedisStore(opts *redis.Options, ttl time.Duration, log *slog.Logger) (*RedisStore, error) {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	if log == nil {
		log = slog.Default()
	}

	client := redis.NewClient(opts)
	ctx, cancel := context.WithCancel(context.Background())

	probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer probeCancel()
	if err := client.Ping(probeCtx).Err(); err != nil {
		cancel()
		_ = client.Close()
		return nil, fmt.Errorf("连接 Redis 失败: %w", err)
	}

	// A single wildcard subscription covers every bin. Per-bin SUBSCRIBE would
	// mean one connection per open SSE stream.
	ps := client.PSubscribe(ctx, eventsPrefix+"*")
	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	_, err := ps.Receive(readyCtx)
	readyCancel()
	if err != nil {
		cancel()
		_ = ps.Close()
		_ = client.Close()
		return nil, fmt.Errorf("订阅 Redis 事件频道失败: %w", err)
	}

	s := &RedisStore{
		rdb:     client,
		ttl:     ttl,
		now:     time.Now,
		maxReqs: MaxRequestsPerSession,
		maxBins: MaxSessions,
		log:     log,
		ctx:     ctx,
		cancel:  cancel,
		ps:      ps,
		subs:    make(map[string]map[chan *RequestEvent]struct{}),
	}
	s.wg.Add(1)
	go s.run()
	return s, nil
}

func (s *RedisStore) Name() string { return "redis" }

func (s *RedisStore) run() {
	defer s.wg.Done()
	ch := s.ps.Channel()
	for {
		select {
		case <-s.ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			token := strings.TrimPrefix(msg.Channel, eventsPrefix)
			if token == "" {
				continue
			}
			var ev RequestEvent
			if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
				s.log.Warn("无法解析 Redis 事件", "bin", token, "err", err)
				continue
			}
			s.fanout(token, &ev)
		}
	}
}

func (s *RedisStore) fanout(token string, ev *RequestEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.subs[token] {
		select {
		case ch <- ev:
		default:
			// Slow consumer drops the event; SSE clients refetch on reconnect.
		}
	}
}

func (s *RedisStore) Create() (*Session, error) {
	now := s.now()
	expires := now.Add(s.ttl)
	token := randomToken(12)

	for attempt := 0; attempt < 3; attempt++ {
		n, err := createScript.Run(s.ctx, s.rdb,
			[]string{binsKey, metaKey(token)},
			token,
			now.Format(time.RFC3339Nano),
			strconv.FormatInt(expires.UnixMilli(), 10),
			strconv.FormatInt(now.UnixMilli(), 10),
			s.maxBins,
		).Int()
		if err != nil {
			return nil, err
		}
		switch n {
		case -1:
			return nil, ErrTooManySessions
		case 1:
			return &Session{Token: token, Created: now, Expires: expires}, nil
		default:
			token = randomToken(12) // vanishingly unlikely collision
		}
	}
	return nil, errors.New("无法生成唯一的接收箱地址，请稍后再试")
}

// Get relies on Redis TTL for expiry: once the meta key is gone, the bin is gone.
func (s *RedisStore) Get(token string) (*Session, error) {
	vals, err := s.rdb.HGetAll(s.ctx, metaKey(token)).Result()
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, ErrSessionNotFound
	}
	created, err := time.Parse(time.RFC3339Nano, vals["createdAt"])
	if err != nil {
		return nil, fmt.Errorf("损坏的 createdAt: %w", err)
	}
	ms, err := strconv.ParseInt(vals["expiresAt"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("损坏的 expiresAt: %w", err)
	}
	return &Session{
		Token:   vals["token"],
		Created: created,
		Expires: time.UnixMilli(ms),
	}, nil
}

func (s *RedisStore) Add(token string, c *Captured) error {
	meta, err := s.Get(token)
	if err != nil {
		return err
	}
	payload, err := encodeCaptured(c)
	if err != nil {
		return err
	}

	n, err := addScript.Run(s.ctx, s.rdb,
		[]string{idsKey(token), dataKey(token), metaKey(token)},
		c.ID,
		string(payload),
		s.maxReqs,
		strconv.FormatInt(meta.Expires.UnixMilli(), 10),
	).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrSessionNotFound
	}

	// Broadcast a body-free summary so every instance can notify its SSE
	// clients. The request is already durable: a missed push heals when the
	// browser reconnects and refetches the list.
	ev, err := json.Marshal(toEvent(c))
	if err != nil {
		return err
	}
	if err := s.rdb.Publish(s.ctx, eventsChannel(token), string(ev)).Err(); err != nil {
		s.log.Warn("事件推送失败", "bin", token, "err", err)
	}
	return nil
}

func (s *RedisStore) List(token string) ([]*Captured, int, error) {
	if _, err := s.Get(token); err != nil {
		return nil, 0, err
	}

	pipe := s.rdb.Pipeline()
	idsCmd := pipe.LRange(s.ctx, idsKey(token), 0, -1)
	dropCmd := pipe.HGet(s.ctx, metaKey(token), "dropped")
	// Exec can report redis.Nil when the dropped field is absent; individual
	// command results are still populated, so check those instead.
	_, _ = pipe.Exec(s.ctx)

	ids, err := idsCmd.Result()
	if err != nil {
		return nil, 0, err
	}
	dropped := 0
	if v, err := dropCmd.Int(); err == nil {
		dropped = v
	}

	out := make([]*Captured, 0, len(ids))
	if len(ids) == 0 {
		return out, dropped, nil
	}
	vals, err := s.rdb.HMGet(s.ctx, dataKey(token), ids...).Result()
	if err != nil {
		return nil, 0, err
	}
	for _, v := range vals {
		raw, ok := v.(string)
		if !ok {
			continue // evicted between LRANGE and HMGET
		}
		c, err := decodeCaptured([]byte(raw))
		if err != nil {
			s.log.Warn("无法解析已存储的请求", "bin", token, "err", err)
			continue
		}
		out = append(out, c)
	}
	return out, dropped, nil
}

func (s *RedisStore) ByID(token, id string) (*Captured, error) {
	if _, err := s.Get(token); err != nil {
		return nil, err
	}
	raw, err := s.rdb.HGet(s.ctx, dataKey(token), id).Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrRequestNotFound
	}
	if err != nil {
		return nil, err
	}
	return decodeCaptured([]byte(raw))
}

// Subscribe registers a local SSE consumer. There is no per-bin Redis
// subscription: the store-level wildcard PSUBSCRIBE already receives every
// event and fans it out here.
func (s *RedisStore) Subscribe(token string) (<-chan *RequestEvent, func(), error) {
	if _, err := s.Get(token); err != nil {
		return nil, nil, err
	}
	ch := make(chan *RequestEvent, subscriberBuffer)

	s.mu.Lock()
	if s.subs[token] == nil {
		s.subs[token] = make(map[chan *RequestEvent]struct{})
	}
	s.subs[token][ch] = struct{}{}
	s.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			s.mu.Lock()
			delete(s.subs[token], ch)
			if len(s.subs[token]) == 0 {
				delete(s.subs, token)
			}
			s.mu.Unlock()
			close(ch)
		})
	}
	return ch, cancel, nil
}

// GC only prunes the bookkeeping ZSET; the bin keys themselves expire in Redis.
func (s *RedisStore) GC() int {
	n, err := s.rdb.ZRemRangeByScore(s.ctx, binsKey,
		"-inf", strconv.FormatInt(s.now().UnixMilli(), 10)).Result()
	if err != nil {
		s.log.Warn("清理过期接收箱失败", "err", err)
		return 0
	}
	return int(n)
}

func (s *RedisStore) Count() int {
	n, err := s.rdb.ZCount(s.ctx, binsKey,
		strconv.FormatInt(s.now().UnixMilli(), 10), "+inf").Result()
	if err != nil {
		return 0
	}
	return int(n)
}

func (s *RedisStore) Close() error {
	s.cancel()
	if s.ps != nil {
		_ = s.ps.Close()
	}
	s.wg.Wait()
	// Local subscriber channels are closed by their own cancel funcs when the
	// HTTP request ends; closing them here too would double-close.
	return s.rdb.Close()
}
