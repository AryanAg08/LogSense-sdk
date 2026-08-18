// Package dtos holds the data-transfer types exchanged with the LogSense API
// and the value structs that make up a client's state.
package dtos

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// LogEvent is a single log record as sent to the ingest endpoint.
type LogEvent struct {
	Source      string         `json:"source"`
	Service     string         `json:"service"`
	Environment string         `json:"environment"`
	Level       string         `json:"level"`
	Message     string         `json:"message"`
	Structured  map[string]any `json:"structured,omitempty"`
	TraceID     string         `json:"traceID,omitempty"`
	Timestamp   *time.Time     `json:"timestamp,omitempty"`
}

// SpanEvent is a single span as sent to the traces ingest endpoint. It mirrors
// the server's native span shape; trace/span IDs are W3C-style hex strings.
type SpanEvent struct {
	Source        string         `json:"source"`
	TraceID       string         `json:"traceID"`
	SpanID        string         `json:"spanID"`
	ParentSpanID  string         `json:"parentSpanID,omitempty"`
	Name          string         `json:"name"`
	Service       string         `json:"service"`
	Environment   string         `json:"environment"`
	Kind          string         `json:"kind,omitempty"`
	StatusCode    string         `json:"statusCode,omitempty"`
	StatusMessage string         `json:"statusMessage,omitempty"`
	StartTime     *time.Time     `json:"startTime,omitempty"`
	EndTime       *time.Time     `json:"endTime,omitempty"`
	DurationMs    float64        `json:"durationMs,omitempty"`
	Attributes    map[string]any `json:"attributes,omitempty"`
}

// Config holds the user-tunable settings for a client: where to send, what to
// stamp on events, and how to batch/retry delivery. Options mutate this.
type Config struct {
	APIKey   string
	Endpoint string
	Service  string
	Env      string

	HTTPClient *http.Client
	OnError    func(error)
	Enricher   func(ctx context.Context) (traceID string, fields map[string]any)

	BatchSize  int
	FlushEvery time.Duration
	MaxRetries int
}

// Stream holds the channels the single sender goroutine communicates over:
// the bounded log + span queues plus flush/stop coordination.
type Stream struct {
	Events   chan LogEvent
	Spans    chan SpanEvent
	FlushReq chan chan struct{}
	Stop     chan struct{}
	Stopped  chan struct{}
}

// Stats holds a client's mutable runtime state. Dropped is accessed atomically.
type Stats struct {
	Dropped  int64 // atomic — events discarded because the queue was full
	StopOnce sync.Once
}
