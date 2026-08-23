package logsense

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/AryanAg08/logsense-sdk/constants"
	"github.com/AryanAg08/logsense-sdk/dtos"
)

type capturedTraces struct {
	mu    sync.Mutex
	spans []dtos.SpanEvent
	logs  []dtos.LogEvent
}

// traceServer accepts both the logs and traces batch endpoints so a single test
// can assert span delivery and log/span correlation.
func traceServer(t *testing.T, rec *capturedTraces) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ai-service/v1/traces/batch":
			var req struct {
				Spans []dtos.SpanEvent `json:"spans"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			rec.mu.Lock()
			rec.spans = append(rec.spans, req.Spans...)
			rec.mu.Unlock()
		case "/ai-service/v1/logs/batch":
			var req struct {
				Logs []dtos.LogEvent `json:"logs"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			rec.mu.Lock()
			rec.logs = append(rec.logs, req.Logs...)
			rec.mu.Unlock()
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
}

// A started+ended span is delivered to /v1/traces/batch with its fields set.
func TestSpan_DeliveredWithFields(t *testing.T) {
	rec := &capturedTraces{}
	srv := traceServer(t, rec)
	defer srv.Close()

	c := New("ls_live_test", WithEndpoint(srv.URL+"/ai-service"), WithService("svc"), WithEnvironment("test"))
	_, span := c.StartSpan(context.Background(), "GET /checkout", WithSpanKind("server"))
	span.SetAttributes(map[string]any{"http.method": "GET"})
	span.End()
	c.Flush()
	c.Shutdown()

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(rec.spans))
	}
	s := rec.spans[0]
	if s.Name != "GET /checkout" || s.Service != "svc" || s.Environment != "test" {
		t.Errorf("span envelope wrong: %+v", s)
	}
	if s.Source != constants.SourceGo {
		t.Errorf("source = %q", s.Source)
	}
	if len(s.TraceID) != constants.TraceIDBytes*2 || len(s.SpanID) != constants.SpanIDBytes*2 {
		t.Errorf("ids wrong length: trace=%q span=%q", s.TraceID, s.SpanID)
	}
	if s.Kind != "server" {
		t.Errorf("kind = %q", s.Kind)
	}
	if s.DurationMs < 0 {
		t.Errorf("durationMs negative: %v", s.DurationMs)
	}
	if s.Attributes["http.method"] != "GET" {
		t.Errorf("attributes lost: %+v", s.Attributes)
	}
}

// A child span inherits its parent's trace ID and records the parent span ID.
func TestSpan_NestedInheritsTrace(t *testing.T) {
	rec := &capturedTraces{}
	srv := traceServer(t, rec)
	defer srv.Close()

	c := New("k", WithEndpoint(srv.URL+"/ai-service"), WithService("svc"))
	ctx, parent := c.StartSpan(context.Background(), "parent")
	_, child := c.StartSpan(ctx, "child")
	child.End()
	parent.End()
	c.Flush()
	c.Shutdown()

	rec.mu.Lock()
	defer rec.mu.Unlock()
	byName := map[string]dtos.SpanEvent{}
	for _, s := range rec.spans {
		byName[s.Name] = s
	}
	p, okP := byName["parent"]
	ch, okC := byName["child"]
	if !okP || !okC {
		t.Fatalf("missing parent/child spans: %+v", rec.spans)
	}
	if ch.TraceID != p.TraceID {
		t.Errorf("child trace %q != parent trace %q", ch.TraceID, p.TraceID)
	}
	if ch.ParentSpanID != p.SpanID {
		t.Errorf("child.parentSpanID %q != parent.spanID %q", ch.ParentSpanID, p.SpanID)
	}
	if p.ParentSpanID != "" {
		t.Errorf("root span should have no parent, got %q", p.ParentSpanID)
	}
}

// A log emitted inside a span is stamped with the span's trace ID.
func TestSpan_LogCorrelation(t *testing.T) {
	rec := &capturedTraces{}
	srv := traceServer(t, rec)
	defer srv.Close()

	c := New("k", WithEndpoint(srv.URL+"/ai-service"), WithService("svc"))
	ctx, span := c.StartSpan(context.Background(), "op")
	c.Log(ctx, "info", "inside span")
	span.End()
	c.Flush()
	c.Shutdown()

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.logs) != 1 {
		t.Fatalf("want 1 log, got %d", len(rec.logs))
	}
	if rec.logs[0].TraceID != span.TraceID() {
		t.Errorf("log trace %q != span trace %q", rec.logs[0].TraceID, span.TraceID())
	}
}

// End is idempotent: calling it twice delivers only one span.
func TestSpan_EndIdempotent(t *testing.T) {
	rec := &capturedTraces{}
	srv := traceServer(t, rec)
	defer srv.Close()

	c := New("k", WithEndpoint(srv.URL+"/ai-service"), WithService("svc"))
	_, span := c.StartSpan(context.Background(), "op")
	span.End()
	span.End()
	c.Flush()
	c.Shutdown()

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.spans) != 1 {
		t.Fatalf("want 1 span after double End, got %d", len(rec.spans))
	}
}

// The package-level StartSpan is safe before Init: nil span, End() no-ops.
func TestSpan_PackageLevelBeforeInitSafe(t *testing.T) {
	ctx, span := StartSpan(context.Background(), "noop")
	if span != nil {
		t.Errorf("expected nil span before Init")
	}
	if ctx == nil {
		t.Errorf("expected original context back")
	}
	span.End() // must not panic
	span.SetError(context.Canceled)
	if span.TraceID() != "" {
		t.Errorf("nil span TraceID should be empty")
	}
}
