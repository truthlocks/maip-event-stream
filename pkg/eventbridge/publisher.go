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

// Package eventbridge implements the MAIP event stream interfaces for
// AWS EventBridge, supporting batched PutEvents calls with detail-type
// routing for receipt event fan-out.
package eventbridge

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// eventBridgeMaxBatch is the maximum entries per PutEvents API call.
const eventBridgeMaxBatch = 10

// EventBridgeAPI abstracts the subset of the AWS EventBridge client used by
// this package. Production code supplies the real SDK client; tests supply
// a mock.
type EventBridgeAPI interface {
	PutEvents(ctx context.Context, input *PutEventsInput) (*PutEventsOutput, error)
}

// PutEventsInput mirrors the AWS SDK PutEventsInput for decoupling.
type PutEventsInput struct {
	Entries []PutEventsRequestEntry
}

// PutEventsOutput mirrors the AWS SDK PutEventsOutput for decoupling.
type PutEventsOutput struct {
	FailedEntryCount int
	Entries          []PutEventsResultEntry
}

// PutEventsRequestEntry mirrors a single entry in a PutEvents call.
type PutEventsRequestEntry struct {
	Source       string
	DetailType   string
	Detail       string
	EventBusName string
}

// PutEventsResultEntry mirrors a single result entry from PutEvents.
type PutEventsResultEntry struct {
	EventID      string
	ErrorCode    string
	ErrorMessage string
}

// Publisher sends MAIP events to an AWS EventBridge event bus. It batches
// events into groups of up to 10 (the EventBridge PutEvents limit) and
// routes them using the configured detail-type.
type Publisher struct {
	client     EventBridgeAPI
	busName    string
	source     string
	detailType string
	logger     *slog.Logger
	mu         sync.Mutex
	closed     bool
}

// NewPublisher creates an EventBridge publisher. The client parameter must
// implement EventBridgeAPI; in production this wraps the real AWS SDK client.
func NewPublisher(client EventBridgeAPI, cfg stream.StreamConfig) *Publisher {
	source := cfg.EventBridgeSource
	if source == "" {
		source = "maip.event-stream"
	}
	detailType := cfg.DetailType
	if detailType == "" {
		detailType = "MAIPReceipt"
	}
	busName := cfg.EventBusName
	if busName == "" {
		busName = "maip-events"
	}

	return &Publisher{
		client:     client,
		busName:    busName,
		source:     source,
		detailType: detailType,
		logger:     slog.Default().With("component", "eventbridge-publisher"),
	}
}

// NewPublisherFromConfig creates an EventBridge publisher from a StreamConfig,
// suitable for use with the backend registry.
func NewPublisherFromConfig(cfg stream.StreamConfig) (stream.Publisher, error) {
	if cfg.EventBusName == "" {
		return nil, fmt.Errorf("eventbridge: event bus name is required")
	}
	// In production, the real AWS SDK client would be injected here using
	// aws-sdk-go-v2/config.LoadDefaultConfig and eventbridge.NewFromConfig.
	// This factory returns an error to signal that a client must be provided
	// via NewPublisher for non-test usage.
	return nil, fmt.Errorf("eventbridge: use NewPublisher with an explicit EventBridgeAPI client")
}

// Publish sends one or more events to EventBridge. Events are batched into
// groups of 10 per API call. Returns an error if any batch fails entirely.
func (p *Publisher) Publish(ctx context.Context, events ...stream.Event) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("eventbridge: publisher is closed")
	}
	p.mu.Unlock()

	for i := 0; i < len(events); i += eventBridgeMaxBatch {
		end := i + eventBridgeMaxBatch
		if end > len(events) {
			end = len(events)
		}
		chunk := events[i:end]

		entries := make([]PutEventsRequestEntry, 0, len(chunk))
		for _, evt := range chunk {
			detail, err := json.Marshal(evt)
			if err != nil {
				p.logger.Error("failed to marshal event", "event_id", evt.ID, "error", err)
				return fmt.Errorf("eventbridge: marshal event %s: %w", evt.ID, err)
			}
			entries = append(entries, PutEventsRequestEntry{
				Source:       p.source,
				DetailType:   p.detailType,
				Detail:       string(detail),
				EventBusName: p.busName,
			})
		}

		output, err := p.client.PutEvents(ctx, &PutEventsInput{Entries: entries})
		if err != nil {
			p.logger.Error("PutEvents failed", "batch_size", len(entries), "error", err)
			return fmt.Errorf("eventbridge: PutEvents: %w", err)
		}
		if output.FailedEntryCount > 0 {
			for _, entry := range output.Entries {
				if entry.ErrorCode != "" {
					p.logger.Warn("event entry failed",
						"error_code", entry.ErrorCode,
						"error_message", entry.ErrorMessage,
					)
				}
			}
			return fmt.Errorf("eventbridge: %d of %d entries failed", output.FailedEntryCount, len(entries))
		}
	}

	return nil
}

// PublishBatch sends a batch of events and returns granular success/failure
// counts. Unlike Publish, partial failures do not cause an error return;
// the caller inspects the counts instead.
func (p *Publisher) PublishBatch(ctx context.Context, events []stream.Event) (int, int, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return 0, len(events), fmt.Errorf("eventbridge: publisher is closed")
	}
	p.mu.Unlock()

	totalSuccess := 0
	totalFailed := 0

	for i := 0; i < len(events); i += eventBridgeMaxBatch {
		end := i + eventBridgeMaxBatch
		if end > len(events) {
			end = len(events)
		}
		chunk := events[i:end]

		entries := make([]PutEventsRequestEntry, 0, len(chunk))
		for _, evt := range chunk {
			detail, err := json.Marshal(evt)
			if err != nil {
				p.logger.Error("failed to marshal event", "event_id", evt.ID, "error", err)
				totalFailed += len(chunk) - len(entries)
				break
			}
			entries = append(entries, PutEventsRequestEntry{
				Source:       p.source,
				DetailType:   p.detailType,
				Detail:       string(detail),
				EventBusName: p.busName,
			})
		}

		if len(entries) == 0 {
			totalFailed += len(chunk)
			continue
		}

		output, err := p.client.PutEvents(ctx, &PutEventsInput{Entries: entries})
		if err != nil {
			p.logger.Error("PutEvents batch failed", "batch_size", len(entries), "error", err)
			totalFailed += len(entries)
			continue
		}

		totalFailed += output.FailedEntryCount
		totalSuccess += len(entries) - output.FailedEntryCount
	}

	return totalSuccess, totalFailed, nil
}

// Close marks the publisher as closed. Subsequent Publish calls will return
// an error.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.logger.Info("publisher closed")
	return nil
}

func init() {
	stream.Register(stream.BackendEntry{
		Name:         stream.BackendEventBridge,
		NewPublisher: NewPublisherFromConfig,
	})
}
