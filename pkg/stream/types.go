// Copyright 2026 Truthlocks Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package stream defines the core interfaces and types for MAIP event streaming.
// It provides a unified abstraction over multiple stream backends (EventBridge,
// Kinesis, Redis Streams, NATS JetStream) allowing receipt events to flow through
// any supported transport without application code changes.
package stream

import (
	"context"
	"encoding/json"
	"time"
)

// Event represents a single event flowing through the stream. It is the
// canonical wire format used by all backend publishers and subscribers.
type Event struct {
	// ID is a globally unique identifier for this event (typically a UUID).
	ID string `json:"id"`
	// Type classifies the event, e.g. "receipt.created", "receipt.verified".
	Type string `json:"type"`
	// Source identifies the originating service, e.g. "maip/machine-identity-service".
	Source string `json:"source"`
	// TenantID is the owning tenant for multi-tenant routing and filtering.
	TenantID string `json:"tenant_id"`
	// Payload carries the event body as raw JSON to preserve schema flexibility.
	Payload json.RawMessage `json:"payload"`
	// Metadata holds arbitrary key-value pairs for tracing, versioning, etc.
	Metadata map[string]string `json:"metadata"`
	// Timestamp records when the event was produced.
	Timestamp time.Time `json:"timestamp"`
}

// ReceiptEvent is a typed wrapper for receipt-specific events. It embeds Event
// and adds fields that are present on every MAIP receipt, enabling type-safe
// access without deserializing the full payload.
type ReceiptEvent struct {
	Event
	// ReceiptID is the unique identifier of the MAIP receipt.
	ReceiptID string `json:"receipt_id"`
	// ReceiptType classifies the receipt (e.g. "data-flow", "delegation", "attestation").
	ReceiptType string `json:"receipt_type"`
	// ChainHash is the SHA-256 hash linking this receipt to the prior receipt in the chain.
	ChainHash string `json:"chain_hash"`
	// AgentID identifies the machine agent that produced or consumed the receipt.
	AgentID string `json:"agent_id"`
}

// Publisher sends events to a stream backend. Implementations must be safe
// for concurrent use by multiple goroutines.
type Publisher interface {
	// Publish sends one or more events to the backend. It returns an error
	// if any event fails to publish. For partial-failure semantics, use
	// PublishBatch instead.
	Publish(ctx context.Context, events ...Event) error

	// PublishBatch sends a batch of events and reports granular success/failure
	// counts. The returned error is non-nil only for systemic failures; partial
	// failures are reflected in the counts.
	PublishBatch(ctx context.Context, events []Event) (successes int, failures int, err error)

	// Close releases resources held by the publisher (connections, buffers).
	Close() error
}

// Subscriber receives events from a stream backend. Implementations must be
// safe for concurrent use by multiple goroutines.
type Subscriber interface {
	// Subscribe begins consuming events, invoking handler for each received
	// event. It blocks until ctx is cancelled or a fatal error occurs.
	Subscribe(ctx context.Context, handler EventHandler) error

	// Acknowledge confirms that the given event IDs have been processed
	// successfully. Backends that do not require explicit ACKs may treat
	// this as a no-op.
	Acknowledge(ctx context.Context, eventIDs ...string) error

	// Close stops the subscriber and releases its resources.
	Close() error
}

// EventHandler processes a single incoming event. Returning a non-nil error
// signals the subscriber to retry or negative-acknowledge the event, depending
// on backend semantics.
type EventHandler func(ctx context.Context, event Event) error

// Middleware wraps a Publisher with additional cross-cutting behavior such as
// retries, deduplication, or metrics collection. Middleware functions are
// listed outermost first: ApplyMiddleware(pub, retry, metrics) means retry
// wraps metrics, and metrics wraps the inner publisher.
type Middleware func(Publisher) Publisher

// ApplyMiddleware chains zero or more middleware functions around a base
// publisher. The first middleware in the slice is the outermost wrapper.
func ApplyMiddleware(pub Publisher, mws ...Middleware) Publisher {
	for i := len(mws) - 1; i >= 0; i-- {
		pub = mws[i](pub)
	}
	return pub
}
