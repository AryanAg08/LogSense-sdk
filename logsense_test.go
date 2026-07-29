package logsense

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/AryanAg08/logsense-sdk/constants"
	"github.com/AryanAg08/logsense-sdk/dtos"
	"time"
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
	// #7: message must be stable (no stack embedded) so occurrences group.
	if errEvt.Message != "redis GET auth:token -> nil" {
		t.Errorf("message not stable: %q", errEvt.Message)
	}
	if _, ok := errEvt.Structured["stack"]; !ok {
		t.Errorf("stack should be in structured, not message")
	}
	if errEvt.Structured["path"] != "/login" {
		t.Errorf("extra fields lost: %+v", errEvt.Structured)
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
