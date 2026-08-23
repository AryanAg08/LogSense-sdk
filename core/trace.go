package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/AryanAg08/logsense-sdk/constants"
	"github.com/AryanAg08/logsense-sdk/dtos"
)

// spanCtxKey is the private context key under which the active span is carried,
// so a child StartSpan can inherit its parent's trace and span IDs.
type spanCtxKey struct{}

// Span is an in-progress unit of work. Create one with Client.StartSpan, then
// call End (typically deferred) to record its duration and enqueue it for
// delivery. A Span is safe to mutate from the goroutine that owns it; End is
// idempotent.
type Span struct {
	client  *Client
	ev      dtos.SpanEvent
	start   time.Time
	endOnce sync.Once
	mu      sync.Mutex
}

// SpanOption configures a Span at creation.
type SpanOption func(*Span)

// WithSpanKind sets the span kind (server|client|producer|consumer|internal).
func WithSpanKind(kind string) SpanOption {
	return func(s *Span) { s.ev.Kind = kind }
}

// WithSpanAttributes attaches initial attributes to the span.
func WithSpanAttributes(attrs map[string]any) SpanOption {
	return func(s *Span) { s.SetAttributes(attrs) }
}

// StartSpan begins a span named name. If ctx already carries a span, the new one
// inherits its trace ID and becomes its child; otherwise a new trace is started.
// The returned context carries the new span so nested StartSpan calls link up.
//
// Always end the span:
//
//	ctx, span := client.StartSpan(ctx, "GET /checkout")
//	defer span.End()
func (c *Client) StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, *Span) {
	if ctx == nil {
		ctx = context.Background()
	}

	traceID := newHexID(constants.TraceIDBytes)
	parentSpanID := ""
	if parent, ok := ctx.Value(spanCtxKey{}).(*Span); ok && parent != nil {
		traceID = parent.ev.TraceID
		parentSpanID = parent.ev.SpanID
	}

	now := time.Now()
	s := &Span{
		client: c,
		start:  now,
		ev: dtos.SpanEvent{
			Source:       constants.SourceGo,
			TraceID:      traceID,
			SpanID:       newHexID(constants.SpanIDBytes),
			ParentSpanID: parentSpanID,
			Name:         name,
			Service:      c.cfg.Service,
			Environment:  c.cfg.Env,
			StartTime:    &now,
		},
	}
	for _, o := range opts {
		o(s)
	}
	return context.WithValue(ctx, spanCtxKey{}, s), s
}

// SetAttributes merges attributes into the span (last write wins).
func (s *Span) SetAttributes(attrs map[string]any) {
	if s == nil || len(attrs) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ev.Attributes == nil {
		s.ev.Attributes = make(map[string]any, len(attrs))
	}
	for k, v := range attrs {
		s.ev.Attributes[k] = v
	}
}

// SetStatus sets the span's status code (e.g. "OK", "ERROR") and message.
func (s *Span) SetStatus(code, message string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ev.StatusCode = code
	s.ev.StatusMessage = message
}

// SetError marks the span as failed: status ERROR with err's message, and an
// "error" attribute. No-op when err is nil.
func (s *Span) SetError(err error) {
	if s == nil || err == nil {
		return
	}
	s.SetStatus("ERROR", err.Error())
	s.SetAttributes(map[string]any{"error": err.Error()})
}

// TraceID returns the span's trace ID (useful for correlating logs).
func (s *Span) TraceID() string {
	if s == nil {
		return ""
	}
	return s.ev.TraceID
}

// SpanID returns the span's ID.
func (s *Span) SpanID() string {
	if s == nil {
		return ""
	}
	return s.ev.SpanID
}

// End records the span's end time and duration and enqueues it for delivery.
// Idempotent — only the first call takes effect. Safe to call on a nil span.
func (s *Span) End() {
	if s == nil {
		return
	}
	s.endOnce.Do(func() {
		end := time.Now()
		s.mu.Lock()
		s.ev.EndTime = &end
		s.ev.DurationMs = float64(end.Sub(s.start).Microseconds()) / 1000.0
		ev := s.ev
		s.mu.Unlock()
		s.client.enqueueSpan(ev)
	})
}

// newHexID returns n cryptographically-random bytes as lowercase hex. On the
// vanishingly unlikely rand failure it falls back to a timestamp so a span still
// gets a non-empty, unique-enough ID rather than a panic.
func newHexID(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		for i := range b {
			b[i] = byte(time.Now().UnixNano() >> (uint(i) * 8))
		}
	}
	return hex.EncodeToString(b)
}

// activeTraceID returns the trace ID of the span carried by ctx, if any. Used to
// correlate logs emitted within a span.
func activeTraceID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if s, ok := ctx.Value(spanCtxKey{}).(*Span); ok && s != nil {
		return s.ev.TraceID
	}
	return ""
}
