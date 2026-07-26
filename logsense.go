// Package logsense is the Go SDK for LogSense.
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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultEndpoint      = "https://api.logsense.cloud/ai-service"
	defaultBatchSize     = 50
	defaultFlushInterval = 2 * time.Second
	defaultTimeout       = 5 * time.Second
	defaultMaxQueue      = 10000
	defaultMaxRetries    = 3

	sourceGo = "sdk-go"

	// Per-event caps keep any single event well under the server's 64KB limit,
	// so a giant message or stack can't produce a request the server rejects.
	maxMessageBytes = 16 * 1024
	maxStackBytes   = 16 * 1024
)

// Client is the LogSense SDK client. Create one with New() or use the package-level Init().
//
// A single background goroutine owns delivery: events flow through a bounded
// channel to one sender, so there are never concurrent in-flight batches and
// memory can't grow without bound if the endpoint is slow or down.
type Client struct {
	apiKey   string
	endpoint string
	service  string
	env      string

	httpClient *http.Client
	onError    func(error)
	enricher   func(ctx context.Context) (traceID string, fields map[string]any)

	batchSize  int
	flushEvery time.Duration
	maxRetries int

	events   chan logEvent
	flushReq chan chan struct{}
	stop     chan struct{}
	stopped  chan struct{}

	dropped  int64 // atomic — events discarded because the queue was full
	stopOnce sync.Once
}

type logEvent struct {
	Source      string         `json:"source"`
	Service     string         `json:"service"`
	Environment string         `json:"environment"`
	Level       string         `json:"level"`
	Message     string         `json:"message"`
	Structured  map[string]any `json:"structured,omitempty"`
	TraceID     string         `json:"traceID,omitempty"`
	Timestamp   *time.Time     `json:"timestamp,omitempty"`
}

// Option configures a Client.
type Option func(*Client)

// WithEndpoint overrides the default API endpoint.
func WithEndpoint(url string) Option { return func(c *Client) { c.endpoint = url } }

// WithService sets a default service name for all captured logs.
func WithService(name string) Option { return func(c *Client) { c.service = name } }

// WithEnvironment sets a default environment (prod/staging/dev).
func WithEnvironment(env string) Option { return func(c *Client) { c.env = env } }

// WithOnError registers a callback invoked when a batch cannot be delivered
// (marshal failure, network error, or non-2xx response after retries). Use it
// to surface delivery problems — a wrong API key or a 5xx-ing endpoint would
// otherwise lose logs with no signal. The callback must not block.
func WithOnError(fn func(error)) Option { return func(c *Client) { c.onError = fn } }

// WithMaxQueue caps the number of buffered events. When the queue is full new
// events are dropped (drop-newest) and counted; see Dropped. Default 10000.
func WithMaxQueue(n int) Option {
	return func(c *Client) {
		if n > 0 {
			c.events = make(chan logEvent, n)
		}
	}
}

// WithBatchSize sets how many events accumulate before an early flush. Default 50.
func WithBatchSize(n int) Option {
	return func(c *Client) {
		if n > 0 {
			c.batchSize = n
		}
	}
}

// WithHTTPClient supplies a custom *http.Client (for tuned pooling, proxies, or
// tests). By default the SDK uses its own client with a 5s timeout, never the
// shared http.DefaultClient.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		if hc != nil {
			c.httpClient = hc
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
	return func(c *Client) { c.enricher = fn }
}

// New creates a new Client and starts its background sender goroutine.
func New(apiKey string, opts ...Option) *Client {
	c := &Client{
		apiKey:     apiKey,
		endpoint:   defaultEndpoint,
		service:    "unknown",
		env:        "production",
		httpClient: &http.Client{Timeout: defaultTimeout},
		batchSize:  defaultBatchSize,
		flushEvery: defaultFlushInterval,
		maxRetries: defaultMaxRetries,
		events:     make(chan logEvent, defaultMaxQueue),
		flushReq:   make(chan chan struct{}),
		stop:       make(chan struct{}),
		stopped:    make(chan struct{}),
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
		"stack": truncate(string(debug.Stack()), maxStackBytes),
	}
	for _, m := range extra {
		for k, v := range m {
			structured[k] = v
		}
	}
	e := logEvent{
		Source:      sourceGo,
		Service:     c.service,
		Environment: c.env,
		Level:       "error",
		Message:     truncate(err.Error(), maxMessageBytes),
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
	e := logEvent{
		Source:      sourceGo,
		Service:     c.service,
		Environment: c.env,
		Level:       level,
		Message:     truncate(message, maxMessageBytes),
		Structured:  structured,
	}
	c.enrich(ctx, &e)
	c.enqueue(e)
}

// Dropped returns the number of events discarded because the queue was full.
// A non-zero, growing value means the endpoint can't keep up with your volume.
func (c *Client) Dropped() int64 { return atomic.LoadInt64(&c.dropped) }

// Flush sends all currently-buffered events synchronously and waits for the
// send to complete. Safe to call after Shutdown (returns immediately).
func (c *Client) Flush() {
	done := make(chan struct{})
	select {
	case c.flushReq <- done:
		<-done
	case <-c.stopped:
	}
}

// Shutdown flushes any buffered events and stops the background goroutine.
// Idempotent. Always call it (or defer it) before your process exits.
func (c *Client) Shutdown() {
	c.stopOnce.Do(func() { close(c.stop) })
	<-c.stopped
}

func (c *Client) enrich(ctx context.Context, e *logEvent) {
	if c.enricher == nil || ctx == nil {
		return
	}
	traceID, fields := c.enricher(ctx)
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

func (c *Client) enqueue(e logEvent) {
	now := time.Now()
	e.Timestamp = &now
	select {
	case c.events <- e:
	default:
		// Queue full: drop-newest and count it, rather than grow without bound.
		atomic.AddInt64(&c.dropped, 1)
	}
}

// run is the single sender goroutine. It owns the batch slice, so there is
// never more than one in-flight send and the buffer lives in exactly one place.
func (c *Client) run() {
	defer close(c.stopped)
	t := time.NewTicker(c.flushEvery)
	defer t.Stop()

	batch := make([]logEvent, 0, c.batchSize)
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
			case e := <-c.events:
				batch = append(batch, e)
				if len(batch) >= c.batchSize {
					flush()
				}
			default:
				return
			}
		}
	}

	for {
		select {
		case e := <-c.events:
			batch = append(batch, e)
			if len(batch) >= c.batchSize {
				flush()
			}
		case <-t.C:
			flush()
		case done := <-c.flushReq:
			drain()
			flush()
			close(done)
		case <-c.stop:
			drain()
			flush()
			return
		}
	}
}

// httpError carries a non-2xx status so retryable() can decide whether to retry.
type httpError struct{ status int }

func (e *httpError) Error() string { return fmt.Sprintf("server returned status %d", e.status) }

func (c *Client) send(batch []logEvent) {
	type batchReq struct {
		Logs []logEvent `json:"logs"`
	}
	body, err := json.Marshal(batchReq{Logs: batch})
	if err != nil {
		c.reportError(fmt.Errorf("logsense: marshal batch of %d events: %w", len(batch), err))
		return
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
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
	c.reportError(fmt.Errorf("logsense: dropped %d events after %d attempt(s): %w", len(batch), c.maxRetries+1, lastErr))
}

func (c *Client) doSend(body []byte) error {
	req, err := http.NewRequest(http.MethodPost, c.endpoint+"/v1/logs/batch", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.apiKey)

	resp, err := c.httpClient.Do(req)
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
	if c.onError != nil {
		c.onError(err)
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
	d := (200 * time.Millisecond) << uint(attempt-1)
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return d
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…(truncated)"
}

// ─── Package-level convenience API ──────────────────────────────────────────

var (
	defaultClient *Client
	once          sync.Once
)

// Init initialises the package-level client. Call once at startup.
func Init(apiKey string, opts ...Option) {
	once.Do(func() {
		defaultClient = New(apiKey, opts...)
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
