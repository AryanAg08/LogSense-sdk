// Package constants holds the SDK's default configuration values and fixed
// limits, shared by the logsense package.
package constants

import "time"

const (
	DefaultEndpoint      = "https://api.logsense.cloud/ai-service"
	DefaultBatchSize     = 50
	DefaultFlushInterval = 2 * time.Second
	DefaultTimeout       = 5 * time.Second
	DefaultMaxQueue      = 10000
	DefaultMaxRetries    = 3

	SourceGo = "sdk-go"

	// Ingest paths, appended to the configured endpoint.
	LogsBatchPath   = "/v1/logs/batch"
	TracesBatchPath = "/v1/traces/batch"

	// W3C trace-context ID sizes: a 16-byte trace ID and 8-byte span ID, each
	// sent as lowercase hex.
	TraceIDBytes = 16
	SpanIDBytes  = 8

	// Per-event caps keep any single event well under the server's 64KB limit,
	// so a giant message or stack can't produce a request the server rejects.
	MaxMessageBytes = 16 * 1024
	MaxStackBytes   = 16 * 1024
)
