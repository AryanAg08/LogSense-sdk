# LogSense Go SDK

[![Go Reference](https://pkg.go.dev/badge/github.com/AryanAg08/LogSense-sdk.svg)](https://pkg.go.dev/github.com/AryanAg08/LogSense-sdk)
[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue)](https://golang.org/dl/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

The official Go SDK for [LogSense](https://aryangoyal.space) — structured log ingestion with automatic AI-powered error analysis.

---

## Installation

```bash
go get github.com/AryanAg08/LogSense-sdk
```

Requires Go 1.21 or later. No external dependencies — standard library only.

---

## Quick start

```go
package main

import (
    "context"

    logsense "github.com/AryanAg08/LogSense-sdk"
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

Call once at startup. Creates a package-level client shared across your application.

For multiple isolated clients (e.g. different services in one binary):

```go
client := logsense.New(apiKey, opts...)
defer client.Shutdown()
```

### Options

| Option | Description | Default |
|---|---|---|
| `WithService(name string)` | Service name attached to every log | `"unknown"` |
| `WithEnvironment(env string)` | Environment tag (`prod`, `staging`, `dev`) | `"production"` |
| `WithEndpoint(url string)` | Override the LogSense API base URL | `https://services.aryangoyal.space/ai-service` |

### Capture

```go
logsense.Capture(err error, ctx context.Context, extra ...map[string]any)
```

Enqueues an `error`-level log containing the error message, a full stack trace, and any extra structured fields you pass.

### Log

```go
logsense.Log(ctx context.Context, level, message string, fields ...map[string]any)
```

Enqueues a log line at the specified level. Supported levels: `error`, `warn`, `info`, `debug`.

### Flush / Shutdown

```go
logsense.Flush()     // send all buffered events synchronously
logsense.Shutdown()  // flush then stop the background goroutine
```

Always call `Shutdown()` (or `defer logsense.Shutdown()`) before your process exits to avoid dropping buffered events.

---

## Behaviour

| Property | Detail |
|---|---|
| Non-blocking | All calls return immediately — zero latency impact on your application |
| Batched delivery | Events sent every 2 s or when 50 events accumulate, whichever comes first |
| Never panics | Network errors are silently dropped — your application is never interrupted |
| Source tagging | All events are tagged `source: "sdk-go"` so you can distinguish SDK traffic from raw HTTP calls in the dashboard |

---

## Examples

### Gin middleware

Automatically capture every Gin handler error:

```go
func LogSenseMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        c.Next()
        for _, e := range c.Errors {
            logsense.Capture(e.Err, c.Request.Context(), map[string]any{
                "path":   c.FullPath(),
                "method": c.Request.Method,
                "status": c.Writer.Status(),
            })
        }
    }
}
```

### Structured logging

```go
logsense.Log(ctx, "warn", "slow database query", map[string]any{
    "query_ms":   342,
    "collection": "users",
    "query":      bson.M{"email": "..."},
})
```

### Multiple clients

```go
authClient := logsense.New(apiKey, logsense.WithService("auth-service"))
paymentClient := logsense.New(apiKey, logsense.WithService("payment-service"))
defer authClient.Shutdown()
defer paymentClient.Shutdown()
```

---

## Getting an API key

1. Sign up at [LogSense](https://aryangoyal.space)
2. Create an organisation and project
3. Generate an API key under **Project → API Keys**

---

## License

MIT