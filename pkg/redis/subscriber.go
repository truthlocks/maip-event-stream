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

package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// Subscriber consumes events from a Redis Stream using XREADGROUP with
// consumer groups. Delivery is at least once; handlers must be idempotent and
// explicitly acknowledge successful processing via XACK.
type Subscriber struct {
	client       RedisStreamAPI
	streamKey    string
	group        string
	consumerName string
	batchSize    int64
	blockMs      int64
	logger       *slog.Logger

	mu      sync.Mutex
	closed  bool
	running bool
	cancel  context.CancelFunc

	// pendingAcks maps event ID to Redis stream message ID.
	pendingAcks sync.Map
}

// NewSubscriber creates a Redis Streams subscriber that joins (or creates)
// the specified consumer group.
func NewSubscriber(client RedisStreamAPI, cfg stream.StreamConfig) *Subscriber {
	streamKey := cfg.RedisStreamKey
	if streamKey == "" {
		streamKey = "maip:receipts"
	}
	group := cfg.RedisConsumerGroup
	if group == "" {
		group = "maip-consumers"
	}
	consumerName := cfg.RedisConsumerName
	if consumerName == "" {
		consumerName = "consumer-1"
	}
	batchSize := int64(cfg.BatchSize)
	if batchSize <= 0 {
		batchSize = 25
	}

	return &Subscriber{
		client:       client,
		streamKey:    streamKey,
		group:        group,
		consumerName: consumerName,
		batchSize:    batchSize,
		blockMs:      5000,
		logger:       slog.Default().With("component", "redis-subscriber"),
	}
}

// Subscribe begins consuming events. It creates the consumer group if it does
// not exist, then enters a polling loop. It blocks until ctx is cancelled.
func (s *Subscriber) Subscribe(ctx context.Context, handler stream.EventHandler) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("redis: subscriber is closed")
	}
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("redis: subscriber is already running")
	}
	if handler == nil {
		s.mu.Unlock()
		return fmt.Errorf("redis: event handler is required")
	}
	s.running = true
	ctx, s.cancel = context.WithCancel(ctx)
	cancel := s.cancel
	s.mu.Unlock()

	defer func() {
		cancel()
		s.mu.Lock()
		s.running = false
		s.cancel = nil
		s.mu.Unlock()
	}()

	// Only Redis BUSYGROUP means that creation can safely be skipped.
	if err := s.client.XGroupCreateMkStream(ctx, s.streamKey, s.group, "0"); err != nil {
		if !strings.HasPrefix(err.Error(), "BUSYGROUP ") {
			return fmt.Errorf("redis: create consumer group: %w", err)
		}
	}

	s.logger.Info("starting XREADGROUP loop",
		"stream", s.streamKey,
		"group", s.group,
		"consumer", s.consumerName,
	)

	retryDelay := 100 * time.Millisecond
	// Walk this durable consumer's pending entries before reading new entries,
	// then repeat periodically. Advancing the cursor prevents a poison entry
	// from starving later pending messages. Never ACK a failed handler.
	pendingCursor := "0"
	recoverPending := true
	nextRecovery := time.Now().Add(5 * time.Second)
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("subscriber stopped", "reason", ctx.Err())
			return ctx.Err()
		default:
		}

		if !recoverPending && !time.Now().Before(nextRecovery) {
			recoverPending, pendingCursor = true, "0"
		}
		position, block := ">", s.blockMs
		if recoverPending {
			position, block = pendingCursor, -1 // nonblocking pending reads
		}
		streams, err := s.client.XReadGroup(ctx, &XReadGroupArgs{
			Group:    s.group,
			Consumer: s.consumerName,
			Streams:  []string{s.streamKey, position},
			Count:    s.batchSize,
			Block:    block,
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			s.logger.Error("XREADGROUP failed", "error", err)
			timer := time.NewTimer(retryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			retryDelay = min(retryDelay*2, 5*time.Second)
			continue
		}

		retryDelay = 100 * time.Millisecond
		recovered := 0
		for _, xs := range streams {
			for _, msg := range xs.Messages {
				if recoverPending {
					pendingCursor = msg.ID
					recovered++
				}
				event, err := s.parseMessage(msg)
				if err != nil {
					s.logger.Error("failed to parse stream message",
						"stream_id", msg.ID, "error", err,
					)
					continue
				}

				// Map event ID to stream message ID for ACK.
				s.pendingAcks.Store(event.ID, msg.ID)

				if err := handler(ctx, event); err != nil {
					s.logger.Warn("handler returned error",
						"event_id", event.ID, "error", err,
					)
					s.pendingAcks.Delete(event.ID)
					continue
				}
			}
		}
		if recoverPending && recovered == 0 {
			recoverPending = false
			nextRecovery = time.Now().Add(5 * time.Second)
		}
	}
}

// parseMessage extracts a stream.Event from a Redis Streams message.
func (s *Subscriber) parseMessage(msg XMessage) (stream.Event, error) {
	dataStr, ok := msg.Values["data"]
	if !ok {
		return stream.Event{}, fmt.Errorf("message %s missing 'data' field", msg.ID)
	}

	var data string
	switch v := dataStr.(type) {
	case string:
		data = v
	default:
		return stream.Event{}, fmt.Errorf("message %s: 'data' field is not a string", msg.ID)
	}

	var event stream.Event
	if err := json.Unmarshal([]byte(data), &event); err != nil {
		return stream.Event{}, fmt.Errorf("message %s: unmarshal event: %w", msg.ID, err)
	}

	return event, nil
}

// Acknowledge sends XACK for the given event IDs, removing them from the
// consumer's pending entries list.
func (s *Subscriber) Acknowledge(ctx context.Context, eventIDs ...string) error {
	streamIDs := make([]string, 0, len(eventIDs))
	confirmed := make(map[string]string)
	for _, eid := range eventIDs {
		sid, ok := s.pendingAcks.Load(eid)
		if !ok {
			s.logger.Warn("no pending ACK for event", "event_id", eid)
			continue
		}
		streamIDs = append(streamIDs, sid.(string))
		confirmed[eid] = sid.(string)
	}

	if len(streamIDs) == 0 {
		return nil
	}

	acked, err := s.client.XAck(ctx, s.streamKey, s.group, streamIDs...)
	if err != nil {
		return fmt.Errorf("redis: XACK: %w", err)
	}

	for eid, sid := range confirmed {
		s.pendingAcks.CompareAndDelete(eid, sid)
	}
	s.logger.Debug("acknowledged messages", "count", acked)
	return nil
}

// Close stops the subscriber.
func (s *Subscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("subscriber closed")
	return nil
}
