// Package core holds the LogSense client implementation: option wiring, the
// single background sender goroutine, and the retry/backoff delivery logic.
// The top-level logsense package is a thin facade over this package.
package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/AryanAg08/logsense-sdk/constants"
	"github.com/AryanAg08/logsense-sdk/dtos"
)

// Client is the LogSense SDK client. Create one with New().
//
// Its state is split into DTO structs: cfg (user settings), stream (the
// channels the sender goroutine uses), and stats (runtime counters).
//
// A single background goroutine owns delivery: events flow through the bounded
// stream.Events channel to one sender, so there are never concurrent in-flight
// batches and memory can't grow without bound if the endpoint is slow or down.
type Client struct {
	cfg    dtos.Config
	stream dtos.Stream
	stats  dtos.Stats
}

// Option configures a Client.
type Option func(*Client)

// WithEndpoint overrides the default API endpoint.
func WithEndpoint(url string) Option { return func(c *Client) { c.cfg.Endpoint = url } }

// WithService sets a default service name for all captured logs.
func WithService(name string) Option { return func(c *Client) { c.cfg.Service = name } }

// WithEnvironment sets a default environment (prod/staging/dev).
func WithEnvironment(env string) Option { return func(c *Client) { c.cfg.Env = env } }

// WithOnError registers a callback invoked when a batch cannot be delivered
// (marshal failure, network error, or non-2xx response after retries). Use it
// to surface delivery problems — a wrong API key or a 5xx-ing endpoint would
// otherwise lose logs with no signal. The callback must not block.
func WithOnError(fn func(error)) Option { return func(c *Client) { c.cfg.OnError = fn } }

// WithMaxQueue caps the number of buffered events. When the queue is full new
// events are dropped (drop-newest) and counted; see Dropped. Default 10000.
func WithMaxQueue(n int) Option {
	return func(c *Client) {
		if n > 0 {
			c.stream.Events = make(chan dtos.LogEvent, n)
		}
	}
}

// WithBatchSize sets how many events accumulate before an early flush. Default 50.
func WithBatchSize(n int) Option {
	return func(c *Client) {
		if n > 0 {
			c.cfg.BatchSize = n
		}
	}
}

// WithHTTPClient supplies a custom *http.Client (for tuned pooling, proxies, or
// tests). By default the SDK uses its own client with a 5s timeout, never the
// shared http.DefaultClient.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		if hc != nil {
			c.cfg.HTTPClient = hc
		}
	}
}

// WithContextEnricher registers a function that pulls correlation data out of
// the context.Context passed to Capture/Log — typically an OpenTelemetry
// trace/span ID. Returning a non-empty traceID sets the event's TraceID; any
// returned fields are merged into the event's structured data (without
// overwriting explicit fields). This keeps the SDK dependency-free while still
// letting logs correlate with traces.
func WithContextEnricher(fn func(ctx context.Context) (traceID string, fields map[string]any)) Option {
	return func(c *Client) { c.cfg.Enricher = fn }
}

// New creates a new Client and starts its background sender goroutine.
func New(apiKey string, opts ...Option) *Client {
	c := &Client{
		cfg: dtos.Config{
			APIKey:     apiKey,
			Endpoint:   constants.DefaultEndpoint,
			Service:    "unknown",
			Env:        "production",
			HTTPClient: &http.Client{Timeout: constants.DefaultTimeout},
			BatchSize:  constants.DefaultBatchSize,
			FlushEvery: constants.DefaultFlushInterval,
			MaxRetries: constants.DefaultMaxRetries,
		},
		stream: dtos.Stream{
			Events:   make(chan dtos.LogEvent, constants.DefaultMaxQueue),
			FlushReq: make(chan chan struct{}),
			Stop:     make(chan struct{}),
			Stopped:  make(chan struct{}),
		},
	}
	for _, o := range opts {
		o(c)
	}
	go c.run()
	return c
}

// Capture enqueues an error for async ingest. Never panics.
//
// The error message is used verbatim as the event Message so repeated
// occurrences of the same error group together; the stack trace is attached as
// a structured field rather than embedded in the message.
//
// Note: the stack is captured at the point Capture is called. If you call it
// from inside a logging hook, the stack reflects the hook, not the error's
// origin — call Capture at the site where the error is handled for best results.
func (c *Client) Capture(err error, ctx context.Context, extra ...map[string]any) {
	if err == nil {
		return
	}
	structured := map[string]any{
		"error": err.Error(),
		"stack": truncate(string(debug.Stack()), constants.MaxStackBytes),
	}
	for _, m := range extra {
		for k, v := range m {
			structured[k] = v
		}
	}
	e := dtos.LogEvent{
		Source:      constants.SourceGo,
		Service:     c.cfg.Service,
		Environment: c.cfg.Env,
		Level:       "error",
		Message:     truncate(err.Error(), constants.MaxMessageBytes),
		Structured:  structured,
	}
	c.enrich(ctx, &e)
	c.enqueue(e)
}

// Log enqueues a log line at the given level.
func (c *Client) Log(ctx context.Context, level, message string, fields ...map[string]any) {
	var structured map[string]any
	if len(fields) > 0 {
		structured = make(map[string]any)
		for _, m := range fields {
			for k, v := range m {
				structured[k] = v
			}
		}
	}
	e := dtos.LogEvent{
		Source:      constants.SourceGo,
		Service:     c.cfg.Service,
		Environment: c.cfg.Env,
		Level:       level,
		Message:     truncate(message, constants.MaxMessageBytes),
		Structured:  structured,
	}
	c.enrich(ctx, &e)
	c.enqueue(e)
}

// Dropped returns the number of events discarded because the queue was full.
// A non-zero, growing value means the endpoint can't keep up with your volume.
func (c *Client) Dropped() int64 { return atomic.LoadInt64(&c.stats.Dropped) }

// Flush sends all currently-buffered events synchronously and waits for the
// send to complete. Safe to call after Shutdown (returns immediately).
func (c *Client) Flush() {
	done := make(chan struct{})
	select {
	case c.stream.FlushReq <- done:
		<-done
	case <-c.stream.Stopped:
	}
}

// Shutdown flushes any buffered events and stops the background goroutine.
// Idempotent. Always call it (or defer it) before your process exits.
func (c *Client) Shutdown() {
	c.stats.StopOnce.Do(func() { close(c.stream.Stop) })
	<-c.stream.Stopped
}

func (c *Client) enrich(ctx context.Context, e *dtos.LogEvent) {
	if c.cfg.Enricher == nil || ctx == nil {
		return
	}
	traceID, fields := c.cfg.Enricher(ctx)
	if traceID != "" {
		e.TraceID = traceID
	}
	if len(fields) > 0 {
		if e.Structured == nil {
			e.Structured = make(map[string]any, len(fields))
		}
		for k, v := range fields {
			if _, exists := e.Structured[k]; !exists {
				e.Structured[k] = v
			}
		}
	}
}

func (c *Client) enqueue(e dtos.LogEvent) {
	now := time.Now()
	e.Timestamp = &now
	select {
	case c.stream.Events <- e:
	default:
		// Queue full: drop-newest and count it, rather than grow without bound.
		atomic.AddInt64(&c.stats.Dropped, 1)
	}
}

// run is the single sender goroutine. It owns the batch slice, so there is
// never more than one in-flight send and the buffer lives in exactly one place.
func (c *Client) run() {
	defer close(c.stream.Stopped)
	t := time.NewTicker(c.cfg.FlushEvery)
	defer t.Stop()

	batch := make([]dtos.LogEvent, 0, c.cfg.BatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		c.send(batch)
		batch = batch[:0]
	}
	// drain pulls everything currently queued into the batch (flushing whenever
	// it reaches batchSize) so Flush/Shutdown don't leave events behind.
	drain := func() {
		for {
			select {
			case e := <-c.stream.Events:
				batch = append(batch, e)
				if len(batch) >= c.cfg.BatchSize {
					flush()
				}
			default:
				return
			}
		}
	}

	for {
		select {
		case e := <-c.stream.Events:
			batch = append(batch, e)
			if len(batch) >= c.cfg.BatchSize {
				flush()
			}
		case <-t.C:
			flush()
		case done := <-c.stream.FlushReq:
			drain()
			flush()
			close(done)
		case <-c.stream.Stop:
			drain()
			flush()
			return
		}
	}
}

// httpError carries a non-2xx status so retryable() can decide whether to retry.
type httpError struct{ status int }

func (e *httpError) Error() string { return fmt.Sprintf("server returned status %d", e.status) }

func (c *Client) send(batch []dtos.LogEvent) {
	type batchReq struct {
		Logs []dtos.LogEvent `json:"logs"`
	}
	body, err := json.Marshal(batchReq{Logs: batch})
	if err != nil {
		c.reportError(fmt.Errorf("logsense: marshal batch of %d events: %w", len(batch), err))
		return
	}

	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff(attempt))
		}
		lastErr = c.doSend(body)
		if lastErr == nil {
			return
		}
		if !retryable(lastErr) {
			break
		}
	}
	c.reportError(fmt.Errorf("logsense: dropped %d events after %d attempt(s): %w", len(batch), c.cfg.MaxRetries+1, lastErr))
}

func (c *Client) doSend(body []byte) error {
	req, err := http.NewRequest(http.MethodPost, c.cfg.Endpoint+"/v1/logs/batch", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.cfg.APIKey)

	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Drain so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return &httpError{status: resp.StatusCode}
}

func (c *Client) reportError(err error) {
	if c.cfg.OnError != nil {
		c.cfg.OnError(err)
	}
}

// retryable reports whether an error is worth retrying: network/timeout errors
// and server-side 429/5xx are; client errors (400/401/413, …) are not — retrying
// a bad request just wastes calls.
func retryable(err error) bool {
	var he *httpError
	if errors.As(err, &he) {
		return he.status == http.StatusTooManyRequests || he.status >= 500
	}
	return true
}

// backoff returns an exponential delay (200ms, 400ms, 800ms, …) capped at 5s.
func backoff(attempt int) time.Duration {
	const maxDelay = 5 * time.Second
	// A large attempt would overflow the shift into a negative duration, so
	// short-circuit once the delay is guaranteed to exceed the cap anyway.
	if attempt > 30 {
		return maxDelay
	}
	d := (200 * time.Millisecond) << uint(attempt-1)
	if d <= 0 || d > maxDelay {
		d = maxDelay
	}
	return d
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…(truncated)"
}
