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

// Package nats implements the MAIP event stream interfaces for NATS JetStream,
// using subject hierarchies of the form maip.receipts.{tenant}.{type} for
// fine-grained subscription filtering and durable pull consumers for reliable
// delivery.
package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// JetStreamAPI abstracts the NATS JetStream publish operations.
type JetStreamAPI interface {
	Publish(subject string, data []byte) (*PubAck, error)
	PublishAsync(subject string, data []byte) error
	Close() error
}

// PubAck mirrors the NATS JetStream publish acknowledgement.
type PubAck struct {
	Stream   string
	Sequence uint64
}

// Publisher sends MAIP events to NATS JetStream using a subject hierarchy.
// The subject is constructed as maip.receipts.{tenant_id}.{event_type},
// enabling consumers to subscribe to specific tenants or event types via
// wildcards.
type Publisher struct {
	js          JetStreamAPI
	baseSubject string
	logger      *slog.Logger
	mu          sync.Mutex
	closed      bool
}

// NewPublisher creates a NATS JetStream publisher.
func NewPublisher(js JetStreamAPI, cfg stream.StreamConfig) *Publisher {
	baseSubject := cfg.NatsSubject
	if baseSubject == "" {
		baseSubject = "maip.receipts"
	}
	// Strip trailing wildcard tokens for base subject construction.
	baseSubject = strings.TrimSuffix(baseSubject, ".>")
	baseSubject = strings.TrimSuffix(baseSubject, ".*")

	return &Publisher{
		js:          js,
		baseSubject: baseSubject,
		logger:      slog.Default().With("component", "nats-publisher"),
	}
}

// NewPublisherFromConfig creates a NATS publisher from a StreamConfig,
// suitable for use with the backend registry.
func NewPublisherFromConfig(cfg stream.StreamConfig) (stream.Publisher, error) {
	if cfg.NatsURL == "" {
		return nil, fmt.Errorf("nats: NATS_URL is required")
	}
	return nil, fmt.Errorf("nats: use NewPublisher with an explicit JetStreamAPI client")
}

// SubjectToken encodes a tenant or event type as one collision-free subject token.
// ASCII letters, digits, hyphens and underscores remain readable. Every other
// UTF-8 byte is percent encoded, including percent itself. Consumers using exact
// subject filters must use this function too; wildcard subscriptions still work.
func SubjectToken(value string) string {
	var token strings.Builder
	const hex = "0123456789ABCDEF"
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			token.WriteByte(c)
		} else {
			token.WriteByte('%')
			token.WriteByte(hex[c>>4])
			token.WriteByte(hex[c&15])
		}
	}
	return token.String()
}

func (p *Publisher) buildSubject(evt stream.Event) (string, error) {
	if evt.TenantID == "" || evt.Type == "" || evt.ID == "" {
		return "", fmt.Errorf("nats: event ID, tenant and type are required")
	}
	for _, token := range strings.Split(p.baseSubject, ".") {
		if token == "" || strings.ContainsAny(token, "*> \t\r\n") {
			return "", fmt.Errorf("nats: base subject must contain only literal nonempty tokens")
		}
	}
	return fmt.Sprintf("%s.%s.%s", p.baseSubject, SubjectToken(evt.TenantID), SubjectToken(evt.Type)), nil
}

// Publish sends one or more events to NATS JetStream with synchronous
// acknowledgement.
func (p *Publisher) Publish(ctx context.Context, events ...stream.Event) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("nats: publisher is closed")
	}
	p.mu.Unlock()

	// The injected client must bound an in-flight call; check cancellation
	// before starting each new publish.

	for _, evt := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		subject, err := p.buildSubject(evt)
		if err != nil {
			return err
		}
		data, err := json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("nats: marshal event %s: %w", evt.ID, err)
		}

		ack, err := p.js.Publish(subject, data)
		if err != nil {
			p.logger.Error("JetStream publish failed",
				"event_id", evt.ID, "subject", subject, "error", err,
			)
			return fmt.Errorf("nats: publish event %s to %s: %w", evt.ID, subject, err)
		}

		if ack == nil || ack.Stream == "" || ack.Sequence == 0 {
			return fmt.Errorf("nats: server acknowledgement missing for event %s", evt.ID)
		}
		p.logger.Debug("event published",
			"event_id", evt.ID,
			"subject", subject,
			"stream", ack.Stream,
			"sequence", ack.Sequence,
		)
	}

	return nil
}

// PublishBatch reports only synchronous server-confirmed successes. Individual
// failures are reflected in the counts; cancellation stops further requests.
func (p *Publisher) PublishBatch(ctx context.Context, events []stream.Event) (int, int, error) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return 0, len(events), fmt.Errorf("nats: publisher is closed")
	}
	successes, failures := 0, 0
	for i, evt := range events {
		if err := ctx.Err(); err != nil {
			return successes, failures + len(events) - i, err
		}
		if err := p.Publish(ctx, evt); err != nil {
			failures++
		} else {
			successes++
		}
	}
	return successes, failures, nil
}

// Close releases the NATS JetStream connection.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true
	p.logger.Info("publisher closed")
	return p.js.Close()
}

func init() {
	stream.Register(stream.BackendEntry{
		Name:         stream.BackendNATS,
		NewPublisher: NewPublisherFromConfig,
	})
}
