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
// the bounded event queue plus flush/stop coordination.
type Stream struct {
	Events   chan LogEvent
	FlushReq chan chan struct{}
	Stop     chan struct{}
	Stopped  chan struct{}
}

// Stats holds a client's mutable runtime state. Dropped is accessed atomically.
type Stats struct {
	Dropped  int64 // atomic — events discarded because the queue was full
	StopOnce sync.Once
}
