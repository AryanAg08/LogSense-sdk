// Package logsense is the Go SDK for LogSense.
//
// It is a thin facade: the client, options, and delivery logic live in the
// internal core package. This file wires up the public names and a
// package-level default client for the common single-client case.
//
// Usage:
//
//	import logsense "github.com/AryanAg08/logsense-sdk"
//
//	logsense.Init("ls_live_your_api_key",
//	    logsense.WithService("my-service"),
//	    logsense.WithEnvironment("production"),
//	    logsense.WithOnError(func(err error) { log.Println("logsense:", err) }),
//	)
//	defer logsense.Shutdown()
//
//	logsense.Capture(err, context.Background())
//	logsense.Log(ctx, "info", "user signed up", map[string]any{"user_id": "123"})
package logsense

import (
	"context"
	"sync"

	"github.com/AryanAg08/logsense-sdk/core"
)

// Client is the LogSense SDK client. Create one with New() or use the
// package-level Init() for the common single-client case.
type Client = core.Client

// Option configures a Client.
type Option = core.Option

// Span is an in-progress unit of work started with StartSpan; call End to record it.
type Span = core.Span

// SpanOption configures a Span at creation (e.g. WithSpanKind).
type SpanOption = core.SpanOption

// Public constructors, re-exported from core so callers use logsense.New /
// logsense.WithService without importing the core package directly.
var (
	New                 = core.New
	WithEndpoint        = core.WithEndpoint
	WithService         = core.WithService
	WithEnvironment     = core.WithEnvironment
	WithOnError         = core.WithOnError
	WithMaxQueue        = core.WithMaxQueue
	WithBatchSize       = core.WithBatchSize
	WithHTTPClient      = core.WithHTTPClient
	WithContextEnricher = core.WithContextEnricher

	// Span helpers.
	WithSpanKind       = core.WithSpanKind
	WithSpanAttributes = core.WithSpanAttributes
)

// ─── Package-level convenience API ──────────────────────────────────────────

var (
	defaultClient *core.Client
	once          sync.Once
)

// Init initialises the package-level client. Call once at startup.
func Init(apiKey string, opts ...Option) {
	once.Do(func() {
		defaultClient = core.New(apiKey, opts...)
	})
}

// Capture sends an error to LogSense using the package-level client.
func Capture(err error, ctx context.Context, extra ...map[string]any) {
	if defaultClient != nil {
		defaultClient.Capture(err, ctx, extra...)
	}
}

// Log sends a log line using the package-level client.
func Log(ctx context.Context, level, message string, fields ...map[string]any) {
	if defaultClient != nil {
		defaultClient.Log(ctx, level, message, fields...)
	}
}

// StartSpan begins a span using the package-level client. Returns the original
// context and a nil span if Init has not been called, so callers can still
// `defer span.End()` safely.
func StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, *Span) {
	if defaultClient != nil {
		return defaultClient.StartSpan(ctx, name, opts...)
	}
	return ctx, nil
}

// Flush flushes the package-level client.
func Flush() {
	if defaultClient != nil {
		defaultClient.Flush()
	}
}

// Dropped returns the package-level client's dropped-event count.
func Dropped() int64 {
	if defaultClient != nil {
		return defaultClient.Dropped()
	}
	return 0
}

// Shutdown gracefully shuts down the package-level client.
func Shutdown() {
	if defaultClient != nil {
		defaultClient.Shutdown()
	}
}
