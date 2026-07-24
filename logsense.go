// Package logsense is the Go SDK for LogSense.
//
// Usage:
//
//	import logsense "github.com/AryanAg08/logsense-sdk"
//
//	logsense.Init("ls_live_your_api_key",
//	    logsense.WithService("my-service"),
//	    logsense.WithEnvironment("production"),
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
	"fmt"
	"net/http"
	"runtime/debug"
	"sync"
	"time"
)

const (
	defaultEndpoint      = "https://api.logsense.cloud/ai-service"
	defaultBatchSize     = 50
	defaultFlushInterval = 2 * time.Second
	defaultTimeout       = 5 * time.Second
)

// Client is the LogSense SDK client. Create one with New() or use the package-level Init().
type Client struct {
	apiKey   string
	endpoint string
	service  string
	env      string

	mu      sync.Mutex
	buf     []logEvent
	stop    chan struct{}
	stopped chan struct{}
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

// New creates a new Client and starts its background flush goroutine.
func New(apiKey string, opts ...Option) *Client {
	c := &Client{
		apiKey:   apiKey,
		endpoint: defaultEndpoint,
		service:  "unknown",
		env:      "production",
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	for _, o := range opts {
		o(c)
	}
	go c.flushLoop()
	return c
}

// Capture enqueues an error for async ingest. Never panics.
func (c *Client) Capture(err error, ctx context.Context, extra ...map[string]any) {
	if err == nil {
		return
	}
	msg := fmt.Sprintf("%s\n%s", err.Error(), debug.Stack())
	structured := map[string]any{"error": err.Error()}
	for _, m := range extra {
		for k, v := range m {
			structured[k] = v
		}
	}
	c.enqueue(logEvent{
		Source:      "sdk-go",
		Service:     c.service,
		Environment: c.env,
		Level:       "error",
		Message:     msg,
		Structured:  structured,
	})
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
	c.enqueue(logEvent{
		Source:      "sdk-go",
		Service:     c.service,
		Environment: c.env,
		Level:       level,
		Message:     message,
		Structured:  structured,
	})
}

// Flush sends all buffered events synchronously. Call before process exit.
func (c *Client) Flush() {
	c.mu.Lock()
	batch := c.buf
	c.buf = nil
	c.mu.Unlock()
	if len(batch) > 0 {
		c.send(batch)
	}
}

// Shutdown flushes and stops the background goroutine.
func (c *Client) Shutdown() {
	close(c.stop)
	<-c.stopped
	c.Flush()
}

func (c *Client) enqueue(e logEvent) {
	now := time.Now()
	e.Timestamp = &now
	c.mu.Lock()
	c.buf = append(c.buf, e)
	drain := len(c.buf) >= defaultBatchSize
	c.mu.Unlock()
	if drain {
		go c.Flush()
	}
}

func (c *Client) flushLoop() {
	defer close(c.stopped)
	t := time.NewTicker(defaultFlushInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			c.Flush()
		case <-c.stop:
			return
		}
	}
}

func (c *Client) send(batch []logEvent) {
	type batchReq struct {
		Logs []logEvent `json:"logs"`
	}
	body, err := json.Marshal(batchReq{Logs: batch})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/v1/logs/batch", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// ─── Package-level convenience API ──────────────────────────────────────────

var defaultClient *Client
var once sync.Once

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

// Shutdown gracefully shuts down the package-level client.
func Shutdown() {
	if defaultClient != nil {
		defaultClient.Shutdown()
	}
}
