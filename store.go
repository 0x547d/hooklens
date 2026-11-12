package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// DefaultTTL is how long a bin lives before expiring.
	DefaultTTL = 24 * time.Hour

	// MaxRequestsPerSession bounds how many requests a bin keeps. Oldest are dropped.
	MaxRequestsPerSession = 500

	// MaxSessions bounds the total number of live bins.
	MaxSessions = 20000

	// MaxBodyBytes bounds a single captured payload.
	MaxBodyBytes = 1 << 20 // 1 MiB

	subscriberBuffer = 32
)

var (
	ErrSessionNotFound = errors.New("session not found")
	ErrRequestNotFound = errors.New("request not found")
	ErrTooManySessions = errors.New("接收箱数量已达上限，请稍后再试")
)

// Session is a bin's metadata. It is plain data now: the mutable per-bin
// state lives in whichever Store implementation owns it.
type Session struct {
	Token   string    `json:"token"`
	Created time.Time `json:"createdAt"`
	Expires time.Time `json:"expiresAt"`
}

// Captured is one inbound webhook request.
type Captured struct {
	ID            string      `json:"id"`
	Method        string      `json:"method"`
	Path          string      `json:"path"`
	Query         url.Values  `json:"query"`
	Header        http.Header `json:"headers"`
	Body          []byte      `json:"-"`
	Truncated     bool        `json:"truncated"`
	ContentLength int64       `json:"size"`
	RemoteAddr    string      `json:"remoteAddr"`
	ReceivedAt    time.Time   `json:"receivedAt"`
}

// Preview returns a short single-line-ish sample of the body for list views.
func (c *Captured) Preview(n int) string {
	b := c.Body
	if len(b) > n {
		b = b[:n]
	}
	return strings.TrimSpace(strings.ToValidUTF8(string(b), "\uFFFD"))
}

// IsText reports whether the body can be rendered as text rather than base64.
func (c *Captured) IsText() bool {
	if len(c.Body) == 0 {
		return true
	}
	if !utf8.Valid(c.Body) {
		return false
	}
	return !strings.ContainsRune(string(c.Body), 0)
}

// RequestEvent is the payload pushed to SSE clients and across Redis Pub/Sub.
// It deliberately carries no body: a 1 MiB payload must never be broadcast to
// every app instance just because one request arrived.
type RequestEvent struct {
	ID         string      `json:"id"`
	Method     string      `json:"method"`
	Path       string      `json:"path"`
	Query      url.Values  `json:"query"`
	Header     http.Header `json:"headers"`
	Preview    string      `json:"preview"`
	Size       int64       `json:"size"`
	Truncated  bool        `json:"truncated"`
	RemoteAddr string      `json:"remoteAddr"`
	ReceivedAt time.Time   `json:"receivedAt"`
}

func toEvent(c *Captured) *RequestEvent {
	return &RequestEvent{
		ID:         c.ID,
		Method:     c.Method,
		Path:       c.Path,
		Query:      c.Query,
		Header:     c.Header,
		Preview:    c.Preview(160),
		Size:       c.ContentLength,
		Truncated:  c.Truncated,
		RemoteAddr: c.RemoteAddr,
		ReceivedAt: c.ReceivedAt,
	}
}

// Store is the persistence contract. Both implementations (memory and Redis)
// must satisfy it, and the HTTP layer does not know which one it is talking to.
type Store interface {
	Name() string
	Create() (*Session, error)
	Get(token string) (*Session, error)
	Add(token string, c *Captured) error
	List(token string) ([]*Captured, int, error)
	ByID(token, id string) (*Captured, error)
	Subscribe(token string) (<-chan *RequestEvent, func(), error)
	GC() int
	Count() int
	Close() error
}

// storedCaptured is the JSON shape persisted in Redis. Body is a plain string
// when it is valid UTF-8 and base64 otherwise, so binary payloads survive the
// round trip without loss.
type storedCaptured struct {
	ID            string      `json:"id"`
	Method        string      `json:"method"`
	Path          string      `json:"path"`
	Query         url.Values  `json:"query"`
	Header        http.Header `json:"headers"`
	Body          string      `json:"body"`
	BodyEncoding  string      `json:"bodyEncoding,omitempty"`
	Truncated     bool        `json:"truncated"`
	ContentLength int64       `json:"size"`
	RemoteAddr    string      `json:"remoteAddr"`
	ReceivedAt    time.Time   `json:"receivedAt"`
}

func encodeCaptured(c *Captured) ([]byte, error) {
	st := storedCaptured{
		ID:            c.ID,
		Method:        c.Method,
		Path:          c.Path,
		Query:         c.Query,
		Header:        c.Header,
		Truncated:     c.Truncated,
		ContentLength: c.ContentLength,
		RemoteAddr:    c.RemoteAddr,
		ReceivedAt:    c.ReceivedAt,
	}
	if c.IsText() {
		st.Body = string(c.Body)
	} else {
		st.Body = base64.StdEncoding.EncodeToString(c.Body)
		st.BodyEncoding = "base64"
	}
	return json.Marshal(st)
}

func decodeCaptured(b []byte) (*Captured, error) {
	var st storedCaptured
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, err
	}
	var body []byte
	if st.BodyEncoding == "base64" {
		var err error
		body, err = base64.StdEncoding.DecodeString(st.Body)
		if err != nil {
			return nil, err
		}
	} else {
		body = []byte(st.Body)
	}
	return &Captured{
		ID:            st.ID,
		Method:        st.Method,
		Path:          st.Path,
		Query:         st.Query,
		Header:        st.Header,
		Body:          body,
		Truncated:     st.Truncated,
		ContentLength: st.ContentLength,
		RemoteAddr:    st.RemoteAddr,
		ReceivedAt:    st.ReceivedAt,
	}, nil
}

func randomToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing is unrecoverable for a service whose whole job is
		// unguessable URLs.
		panic("hooklens: crypto/rand unavailable: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
