package main

import (
	"slices"
	"sync"
	"time"
)

// MemStore is the original zero-dependency implementation. It is kept as a
// local-development escape hatch (-storage=memory) and as a behavioural
// reference for the test suite. Production always uses RedisStore.
//
// Because it is process-local, its SSE fan-out only reaches clients connected
// to this exact process. RedisStore does not have that limitation.
type MemStore struct {
	ttl      time.Duration
	now      func() time.Time
	maxReqs  int
	mu       sync.RWMutex
	sessions map[string]*memSession
}

type memSession struct {
	Session

	mu      sync.Mutex
	reqs    []*Captured
	dropped int
	subs    map[chan *RequestEvent]struct{}
}

func NewMemStore(ttl time.Duration) *MemStore {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &MemStore{
		ttl:      ttl,
		now:      time.Now,
		maxReqs:  MaxRequestsPerSession,
		sessions: make(map[string]*memSession),
	}
}

func (s *MemStore) Name() string { return "memory" }

// Create makes a new bin and returns it. Token is 16 URL-safe characters.
func (s *MemStore) Create() (*Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.sessions) >= MaxSessions {
		s.gcLocked()
		if len(s.sessions) >= MaxSessions {
			return nil, ErrTooManySessions
		}
	}

	now := s.now()
	sess := &memSession{
		Session: Session{
			Token:   randomToken(12),
			Created: now,
			Expires: now.Add(s.ttl),
		},
		subs: make(map[chan *RequestEvent]struct{}),
	}
	s.sessions[sess.Token] = sess
	return &sess.Session, nil
}

// Get returns a live session or ErrSessionNotFound. Expired bins are pruned lazily.
func (s *MemStore) Get(token string) (*Session, error) {
	s.mu.RLock()
	sess, ok := s.sessions[token]
	s.mu.RUnlock()
	if !ok {
		return nil, ErrSessionNotFound
	}
	if !s.now().Before(sess.Expires) {
		s.mu.Lock()
		if cur, ok := s.sessions[token]; ok && cur == sess {
			delete(s.sessions, token)
		}
		s.mu.Unlock()
		return nil, ErrSessionNotFound
	}
	return &sess.Session, nil
}

// Add records a request against a bin and notifies live subscribers.
func (s *MemStore) Add(token string, c *Captured) error {
	if _, err := s.Get(token); err != nil {
		return err
	}
	s.mu.RLock()
	sess := s.sessions[token]
	s.mu.RUnlock()
	if sess == nil {
		return ErrSessionNotFound
	}

	sess.mu.Lock()
	sess.reqs = append(sess.reqs, c)
	if len(sess.reqs) > s.maxReqs {
		drop := len(sess.reqs) - s.maxReqs
		sess.reqs = append([]*Captured(nil), sess.reqs[drop:]...)
		sess.dropped += drop
	}
	ev := toEvent(c)
	for ch := range sess.subs {
		select {
		case ch <- ev:
		default:
			// Slow consumer: drop the event, the client refetches on reconnect.
		}
	}
	sess.mu.Unlock()
	return nil
}

// List returns requests newest-first plus the number of dropped-but-ejected ones.
func (s *MemStore) List(token string) ([]*Captured, int, error) {
	if _, err := s.Get(token); err != nil {
		return nil, 0, err
	}
	s.mu.RLock()
	entry := s.sessions[token]
	s.mu.RUnlock()
	if entry == nil {
		return nil, 0, ErrSessionNotFound
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	out := make([]*Captured, 0, len(entry.reqs))
	for _, v := range slices.Backward(entry.reqs) {
		out = append(out, v)
	}
	return out, entry.dropped, nil
}

// ByID returns a single captured request belonging to token.
func (s *MemStore) ByID(token, id string) (*Captured, error) {
	if _, err := s.Get(token); err != nil {
		return nil, err
	}
	s.mu.RLock()
	entry := s.sessions[token]
	s.mu.RUnlock()
	if entry == nil {
		return nil, ErrSessionNotFound
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	for _, v := range slices.Backward(entry.reqs) {
		if v.ID == id {
			return v, nil
		}
	}
	return nil, ErrRequestNotFound
}

// Subscribe returns a channel of newly captured requests. The cancel func must
// be called when the consumer goes away.
func (s *MemStore) Subscribe(token string) (<-chan *RequestEvent, func(), error) {
	if _, err := s.Get(token); err != nil {
		return nil, nil, err
	}
	s.mu.RLock()
	sess := s.sessions[token]
	s.mu.RUnlock()
	if sess == nil {
		return nil, nil, ErrSessionNotFound
	}

	ch := make(chan *RequestEvent, subscriberBuffer)
	sess.mu.Lock()
	sess.subs[ch] = struct{}{}
	sess.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			sess.mu.Lock()
			delete(sess.subs, ch)
			sess.mu.Unlock()
			close(ch)
		})
	}
	return ch, cancel, nil
}

// GC removes expired sessions. Returns how many were removed.
func (s *MemStore) GC() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gcLocked()
}

func (s *MemStore) gcLocked() int {
	now := s.now()
	n := 0
	for token, sess := range s.sessions {
		if !now.Before(sess.Expires) {
			delete(s.sessions, token)
			sess.mu.Lock()
			for ch := range sess.subs {
				delete(sess.subs, ch)
				close(ch)
			}
			sess.reqs = nil
			sess.mu.Unlock()
			n++
		}
	}
	return n
}

// Count reports the number of live bins (used by the health endpoint).
func (s *MemStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sessions)
}

func (s *MemStore) Close() error { return nil }
