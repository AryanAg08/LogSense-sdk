# logsense-go

[![Go Reference](https://pkg.go.dev/badge/github.com/AryanAg08/logsense-go.svg)](https://pkg.go.dev/github.com/AryanAg08/logsense-go)
[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue)](https://golang.org/dl/)

The official Go SDK for [LogSense](https://aryangoyal.space) — structured log ingestion with automatic AI-powered error analysis.

---

## Installation

```bash
go get github.com/AryanAg08/logsense-sdk
```

Requires Go 1.21 or later.

---

## Quick start

```go
package main

import (
    "context"

    "github.com/AryanAg08/logsense-go"
)

func main() {
    logsense.Init("ls_live_your_api_key",
        logsense.WithService("my-service"),
        logsense.WithEnvironment("production"),
    )
    defer logsense.Shutdown()

    // Capture an error with full stack trace
    if err := doSomething(); err != nil {
        logsense.Capture(err, context.Background())
    }

    // Send a structured log line
    logsense.Log(context.Background(), "info", "user signed up", map[string]any{
        "user_id": "123",
        "plan":    "free",
    })
}
```

Logs are buffered and flushed every 2 seconds or when the batch reaches 50 events. `Shutdown()` flushes any remaining events before your process exits.

---

## API

### Initialisation

```go
logsense.Init(apiKey string, opts ...Option)
```

Call once at startup. Uses a package-level client shared across your application.

For multiple isolated clients (e.g. different services in one binary):

```go
client := logsense.New(apiKey, opts...)
defer client.Shutdown()
```

### Options

| Option | Description |
|---|---|
| `WithService(name string)` | Default service name attached to every log (default: `"unknown"`) |
| `WithEnvironment(env string)` | Default environment tag (default: `"production"`) |
| `WithEndpoint(url string)` | Override the API base URL |

### Capture

```go
logsense.Capture(err error, ctx context.Context, extra ...map[string]any)
```

Enqueues an `error`-level log with the error message and a full stack trace. Additional key-value pairs can be passed via `extra`.

### Log

```go
logsense.Log(ctx context.Context, level, message string, fields ...map[string]any)
```

Enqueues a log line at the given level. Supported levels: `error`, `warn`, `info`, `debug`.

### Flush / Shutdown

```go
logsense.Flush()     // send buffered events synchronously
logsense.Shutdown()  // flush and stop the background goroutine
```

Always call `Shutdown()` (or `defer logsense.Shutdown()`) before your process exits.

---

## Behaviour

- **Non-blocking** — all calls return immediately; logs are sent in the background.
- **Never panics** — network errors are silently dropped; your application is never affected.
- **Batched** — events are sent as a batch every 2 seconds or when 50 events accumulate, whichever comes first.
- **Source tagging** — every event is tagged `source: "sdk-go"` automatically, distinguishing SDK traffic from raw HTTP ingestion.

---

## Example: Gin middleware

```go
func LogSenseMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        c.Next()
        if len(c.Errors) > 0 {
            for _, e := range c.Errors {
                logsense.Capture(e.Err, c.Request.Context(), map[string]any{
                    "path":   c.FullPath(),
                    "method": c.Request.Method,
                    "status": c.Writer.Status(),
                })
            }
        }
    }
}
```

---

## License

MIT
