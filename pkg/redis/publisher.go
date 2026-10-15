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

// Package redis implements the MAIP event stream interfaces for Redis Streams,
// using XADD with MAXLEN trimming for publishing and XREADGROUP with consumer
// groups for subscribing.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// RedisStreamAPI abstracts the Redis Streams operations used by the publisher
// and subscriber. Production code supplies a go-redis client wrapper; tests
// supply a mock.
type RedisStreamAPI interface {
	XAdd(ctx context.Context, args *XAddArgs) (string, error)
	XReadGroup(ctx context.Context, args *XReadGroupArgs) ([]XStream, error)
	XAck(ctx context.Context, stream, group string, ids ...string) (int64, error)
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) error
	Close() error
}

// XAddArgs mirrors the go-redis XAddArgs.
type XAddArgs struct {
	Stream string
	MaxLen int64
	Approx bool
	ID     string
	Values map[string]interface{}
}

// XReadGroupArgs mirrors the go-redis XReadGroupArgs.
type XReadGroupArgs struct {
	Group    string
	Consumer string
	Streams  []string
	Count    int64
	Block    int64 // milliseconds; negative = nonblocking, 0 = block indefinitely
	NoAck    bool
}

// XStream represents the result of an XREADGROUP call for a single stream.
type XStream struct {
	Stream   string
	Messages []XMessage
}

// XMessage represents a single Redis Streams message.
type XMessage struct {
	ID     string
	Values map[string]interface{}
}

// Publisher sends MAIP events to a Redis Stream via XADD. It applies MAXLEN
// trimming (approximate by default) to cap stream length and prevent unbounded
// memory growth.
type Publisher struct {
	client    RedisStreamAPI
	streamKey string
	maxLen    int64
	logger    *slog.Logger
	mu        sync.Mutex
	closed    bool
}

// NewPublisher creates a Redis Streams publisher.
func NewPublisher(client RedisStreamAPI, cfg stream.StreamConfig) *Publisher {
	streamKey := cfg.RedisStreamKey
	if streamKey == "" {
		streamKey = "maip:receipts"
	}
	maxLen := cfg.RedisMaxLen
	if maxLen == 0 {
		maxLen = 100000
	} else if maxLen < 0 {
		maxLen = 0
	}

	return &Publisher{
		client:    client,
		streamKey: streamKey,
		maxLen:    maxLen,
		logger:    slog.Default().With("component", "redis-publisher"),
	}
}

// NewPublisherFromConfig creates a Redis publisher from a StreamConfig,
// suitable for use with the backend registry.
func NewPublisherFromConfig(cfg stream.StreamConfig) (stream.Publisher, error) {
	if cfg.RedisURL == "" {
		return nil, fmt.Errorf("redis: REDIS_URL is required")
	}
	return nil, fmt.Errorf("redis: use NewPublisher with an explicit RedisStreamAPI client")
}

// Publish sends one or more events to the Redis Stream.
func (p *Publisher) Publish(ctx context.Context, events ...stream.Event) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("redis: publisher is closed")
	}
	p.mu.Unlock()

	for _, evt := range events {
		data, err := json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("redis: marshal event %s: %w", evt.ID, err)
		}

		_, err = p.client.XAdd(ctx, &XAddArgs{
			Stream: p.streamKey,
			MaxLen: p.maxLen,
			Approx: true,
			ID:     "*",
			Values: map[string]interface{}{
				"event_id":  evt.ID,
				"type":      evt.Type,
				"tenant_id": evt.TenantID,
				"data":      string(data),
			},
		})
		if err != nil {
			p.logger.Error("XADD failed", "event_id", evt.ID, "error", err)
			return fmt.Errorf("redis: XADD event %s: %w", evt.ID, err)
		}
	}

	return nil
}

// PublishBatch sends a batch of events and returns granular counts.
func (p *Publisher) PublishBatch(ctx context.Context, events []stream.Event) (int, int, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return 0, len(events), fmt.Errorf("redis: publisher is closed")
	}
	p.mu.Unlock()

	successes := 0
	failures := 0

	for _, evt := range events {
		data, err := json.Marshal(evt)
		if err != nil {
			p.logger.Error("failed to marshal event", "event_id", evt.ID, "error", err)
			failures++
			continue
		}

		_, err = p.client.XAdd(ctx, &XAddArgs{
			Stream: p.streamKey,
			MaxLen: p.maxLen,
			Approx: true,
			ID:     "*",
			Values: map[string]interface{}{
				"event_id":  evt.ID,
				"type":      evt.Type,
				"tenant_id": evt.TenantID,
				"data":      string(data),
			},
		})
		if err != nil {
			p.logger.Error("XADD failed", "event_id", evt.ID, "error", err)
			failures++
			continue
		}
		successes++
	}

	return successes, failures, nil
}

// Close releases the Redis connection.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true
	p.logger.Info("publisher closed")
	return p.client.Close()
}

func init() {
	stream.Register(stream.BackendEntry{
		Name:         stream.BackendRedis,
		NewPublisher: NewPublisherFromConfig,
	})
}
