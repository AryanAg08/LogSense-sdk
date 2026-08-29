package core

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Logsense-tech/logsense-sdk/constants"
	"github.com/Logsense-tech/logsense-sdk/dtos"
)

type captured struct {
	mu      sync.Mutex
	batches [][]dtos.LogEvent
	apiKeys []string
}

func (c *captured) all() []dtos.LogEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []dtos.LogEvent
	for _, b := range c.batches {
		out = append(out, b...)
	}
	return out
}

func newServer(t *testing.T, status int, rec *captured) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ai-service/v1/logs/batch" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		var req struct {
			Logs []dtos.LogEvent `json:"logs"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		rec.mu.Lock()
		rec.batches = append(rec.batches, req.Logs)
		rec.apiKeys = append(rec.apiKeys, r.Header.Get("X-API-Key"))
		rec.mu.Unlock()
		w.WriteHeader(status)
	}))
}

func TestBatchWireFormatAndStableMessage(t *testing.T) {
	rec := &captured{}
	srv := newServer(t, http.StatusAccepted, rec)
	defer srv.Close()

	c := New("ls_live_test", WithEndpoint(srv.URL+"/ai-service"), WithService("svc"), WithEnvironment("test"))
	c.Log(context.Background(), "info", "user signed up", map[string]any{"user_id": "123"})
	c.Capture(errors.New("redis GET auth:token -> nil"), context.Background(), map[string]any{"path": "/login"})
	c.Flush()
	c.Shutdown()

	events := rec.all()
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d", len(events))
	}
	if got := rec.apiKeys[0]; got != "ls_live_test" {
		t.Errorf("X-API-Key = %q", got)
	}

	var info, errEvt *dtos.LogEvent
	for i := range events {
		switch events[i].Level {
		case "info":
			info = &events[i]
		case "error":
			errEvt = &events[i]
		}
	}
	if info == nil || errEvt == nil {
		t.Fatalf("missing info/error event")
	}
	if info.Source != constants.SourceGo || info.Service != "svc" || info.Environment != "test" {
		t.Errorf("info envelope wrong: %+v", info)
	}
	if info.Structured["user_id"] != "123" {
		t.Errorf("structured fields lost: %+v", info.Structured)
	}
	// Message must be stable (no stack embedded) so occurrences group.
	if errEvt.Message != "redis GET auth:token -> nil" {
		t.Errorf("message not stable: %q", errEvt.Message)
	}
	if _, ok := errEvt.Structured["stack"]; !ok {
		t.Errorf("stack should be in structured, not message")
	}
	if errEvt.Structured["path"] != "/login" {
		t.Errorf("extra fields lost: %+v", errEvt.Structured)
	}
	// Every enqueued event should carry a timestamp.
	if info.Timestamp == nil || errEvt.Timestamp == nil {
		t.Errorf("timestamp not set on events")
	}
}

func TestCaptureNilErrorIsNoop(t *testing.T) {
	rec := &captured{}
	srv := newServer(t, http.StatusAccepted, rec)
	defer srv.Close()

	c := New("k", WithEndpoint(srv.URL+"/ai-service"))
	c.Capture(nil, context.Background())
	c.Flush()
	c.Shutdown()

	if got := len(rec.all()); got != 0 {
		t.Errorf("nil error should enqueue nothing, got %d events", got)
	}
}

func TestRetryOn5xxThenSuccess(t *testing.T) {
	rec := &captured{}
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var req struct {
			Logs []dtos.LogEvent `json:"logs"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		rec.mu.Lock()
		rec.batches = append(rec.batches, req.Logs)
		rec.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	var errCount int32
	c := New("k", WithEndpoint(srv.URL+"/ai-service"), WithOnError(func(error) { atomic.AddInt32(&errCount, 1) }))
	c.Log(context.Background(), "info", "hi")
	c.Flush()
	c.Shutdown()

	if atomic.LoadInt32(&calls) != 3 {
		t.Errorf("want 3 attempts (2 fail + 1 ok), got %d", calls)
	}
	if len(rec.all()) != 1 {
		t.Errorf("event not delivered after retries")
	}
	if atomic.LoadInt32(&errCount) != 0 {
		t.Errorf("OnError should not fire on eventual success")
	}
}

func TestNoRetryOn4xxSurfacesError(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusUnauthorized) // 401 = bad API key
	}))
	defer srv.Close()

	var gotErr error
	var mu sync.Mutex
	c := New("bad", WithEndpoint(srv.URL+"/ai-service"), WithOnError(func(e error) {
		mu.Lock()
		gotErr = e
		mu.Unlock()
	}))
	c.Log(context.Background(), "info", "hi")
	c.Flush()
	c.Shutdown()

	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("4xx must not retry; got %d calls", calls)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotErr == nil {
		t.Fatalf("OnError should fire on 401 — silent failure is the whole bug")
	}
}

func TestBoundedQueueDropsAndCounts(t *testing.T) {
	release := make(chan struct{})
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&hits, 1) == 1 {
			<-release // block the single sender on its first send
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := New("k", WithEndpoint(srv.URL+"/ai-service"), WithMaxQueue(2), WithBatchSize(1))
	// First event gets picked up by the sender, which blocks in the handler.
	c.Log(context.Background(), "info", "first")
	time.Sleep(50 * time.Millisecond)
	// Queue capacity is 2; flood well past it while the sender is stuck.
	for i := 0; i < 100; i++ {
		c.Log(context.Background(), "info", "flood")
	}
	if c.Dropped() == 0 {
		t.Errorf("expected dropped events while queue full, got 0")
	}
	close(release)
	c.Shutdown()
}

func TestContextEnricherPopulatesTrace(t *testing.T) {
	rec := &captured{}
	srv := newServer(t, http.StatusAccepted, rec)
	defer srv.Close()

	type ctxKey string
	const k ctxKey = "trace"
	c := New("k", WithEndpoint(srv.URL+"/ai-service"),
		WithContextEnricher(func(ctx context.Context) (string, map[string]any) {
			id, _ := ctx.Value(k).(string)
			return id, map[string]any{"span": "abc"}
		}))
	ctx := context.WithValue(context.Background(), k, "trace-123")
	c.Log(ctx, "info", "hi")
	c.Flush()
	c.Shutdown()

	events := rec.all()
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if events[0].TraceID != "trace-123" {
		t.Errorf("traceID not enriched: %q", events[0].TraceID)
	}
	if events[0].Structured["span"] != "abc" {
		t.Errorf("enricher fields not merged: %+v", events[0].Structured)
	}
}

// Explicit fields must win over enricher-supplied fields of the same key.
func TestEnricherDoesNotOverwriteExplicitFields(t *testing.T) {
	rec := &captured{}
	srv := newServer(t, http.StatusAccepted, rec)
	defer srv.Close()

	c := New("k", WithEndpoint(srv.URL+"/ai-service"),
		WithContextEnricher(func(ctx context.Context) (string, map[string]any) {
			return "", map[string]any{"user_id": "from-enricher"}
		}))
	c.Log(context.Background(), "info", "hi", map[string]any{"user_id": "explicit"})
	c.Flush()
	c.Shutdown()

	events := rec.all()
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if events[0].Structured["user_id"] != "explicit" {
		t.Errorf("enricher overwrote explicit field: %+v", events[0].Structured)
	}
}

func TestDefaultsApplied(t *testing.T) {
	c := New("k")
	defer c.Shutdown()
	if c.cfg.Endpoint != constants.DefaultEndpoint {
		t.Errorf("endpoint default = %q", c.cfg.Endpoint)
	}
	if c.cfg.Service != "unknown" || c.cfg.Env != "production" {
		t.Errorf("service/env defaults wrong: %q/%q", c.cfg.Service, c.cfg.Env)
	}
	if c.cfg.BatchSize != constants.DefaultBatchSize || c.cfg.MaxRetries != constants.DefaultMaxRetries {
		t.Errorf("batch/retry defaults wrong: %d/%d", c.cfg.BatchSize, c.cfg.MaxRetries)
	}
	if cap(c.stream.Events) != constants.DefaultMaxQueue {
		t.Errorf("queue cap default = %d", cap(c.stream.Events))
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	c := New("k", WithEndpoint("http://127.0.0.1:0"))
	c.Shutdown()
	// A second Shutdown must not panic on a double channel close.
	c.Shutdown()
	// Flush after shutdown must return immediately, not deadlock.
	done := make(chan struct{})
	go func() { c.Flush(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Flush after Shutdown deadlocked")
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("short", 10); got != "short" {
		t.Errorf("under-limit changed: %q", got)
	}
	if got := truncate("exactly-ten", len("exactly-ten")); got != "exactly-ten" {
		t.Errorf("at-limit changed: %q", got)
	}
	got := truncate("abcdef", 3)
	if !strings.HasPrefix(got, "abc") || !strings.HasSuffix(got, "(truncated)") {
		t.Errorf("over-limit not truncated correctly: %q", got)
	}
}

func TestRetryableClassification(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{errors.New("dial tcp: connection refused"), true}, // network error
		{&httpError{status: http.StatusTooManyRequests}, true},
		{&httpError{status: http.StatusServiceUnavailable}, true},
		{&httpError{status: http.StatusInternalServerError}, true},
		{&httpError{status: http.StatusBadRequest}, false},
		{&httpError{status: http.StatusUnauthorized}, false},
		{&httpError{status: http.StatusRequestEntityTooLarge}, false},
	}
	for _, tc := range cases {
		if got := retryable(tc.err); got != tc.want {
			t.Errorf("retryable(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	if got := backoff(1); got != 200*time.Millisecond {
		t.Errorf("backoff(1) = %v, want 200ms", got)
	}
	if got := backoff(2); got != 400*time.Millisecond {
		t.Errorf("backoff(2) = %v, want 400ms", got)
	}
	if got := backoff(3); got != 800*time.Millisecond {
		t.Errorf("backoff(3) = %v, want 800ms", got)
	}
	if got := backoff(50); got != 5*time.Second {
		t.Errorf("backoff(50) = %v, want cap 5s", got)
	}
}

func TestLongMessageAndStackAreTruncated(t *testing.T) {
	rec := &captured{}
	srv := newServer(t, http.StatusAccepted, rec)
	defer srv.Close()

	huge := strings.Repeat("x", constants.MaxMessageBytes+5000)
	c := New("k", WithEndpoint(srv.URL+"/ai-service"))
	c.Capture(errors.New(huge), context.Background())
	c.Flush()
	c.Shutdown()

	events := rec.all()
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if len(events[0].Message) > constants.MaxMessageBytes+len("…(truncated)") {
		t.Errorf("message not truncated: %d bytes", len(events[0].Message))
	}
	stack, _ := events[0].Structured["stack"].(string)
	if len(stack) > constants.MaxStackBytes+len("…(truncated)") {
		t.Errorf("stack not truncated: %d bytes", len(stack))
	}
}
