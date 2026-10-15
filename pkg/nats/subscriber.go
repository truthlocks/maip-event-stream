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

package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// JetStreamSubscribeAPI abstracts the NATS JetStream pull-subscribe operations.
type JetStreamSubscribeAPI interface {
	PullSubscribe(subject, durable string) (PullSubscription, error)
}

// PullSubscription abstracts a NATS JetStream pull subscription.
type PullSubscription interface {
	Fetch(batch int, timeout time.Duration) ([]JetStreamMsg, error)
	Unsubscribe() error
}

// JetStreamMsg abstracts a single NATS JetStream message.
type JetStreamMsg interface {
	Data() []byte
	Subject() string
	Ack() error
	Nak() error
	Metadata() (*MsgMetadata, error)
}

// MsgMetadata contains JetStream message metadata.
type MsgMetadata struct {
	Sequence     uint64
	NumDelivered uint64
	NumPending   uint64
	Timestamp    time.Time
	Stream       string
	Consumer     string
}

type pendingMessage struct{ message JetStreamMsg }

// Subscriber consumes events from NATS JetStream using a durable pull
// consumer. Messages are fetched in batches and individually acknowledged
// or negatively acknowledged based on handler outcome.
type Subscriber struct {
	jsAPI        JetStreamSubscribeAPI
	subject      string
	durableName  string
	batchSize    int
	fetchTimeout time.Duration
	logger       *slog.Logger

	mu     sync.Mutex
	closed bool
	cancel context.CancelFunc
	sub    PullSubscription

	// pendingAcks maps event ID to the JetStreamMsg for deferred ACK.
	pendingAcks sync.Map
}

// NewSubscriber creates a NATS JetStream pull subscriber.
func NewSubscriber(jsAPI JetStreamSubscribeAPI, cfg stream.StreamConfig) *Subscriber {
	subject := cfg.NatsSubject
	if subject == "" {
		subject = "maip.receipts.>"
	}
	durableName := cfg.NatsDurableName
	if durableName == "" {
		durableName = "maip-consumer"
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 25
	}

	return &Subscriber{
		jsAPI:        jsAPI,
		subject:      subject,
		durableName:  durableName,
		batchSize:    batchSize,
		fetchTimeout: cfg.FlushInterval,
		logger:       slog.Default().With("component", "nats-subscriber"),
	}
}

// Subscribe begins consuming events from JetStream. It creates a durable
// pull subscription and fetches messages in a loop until ctx is cancelled.
func (s *Subscriber) Subscribe(ctx context.Context, handler stream.EventHandler) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("nats: subscriber is closed")
	}
	ctx, s.cancel = context.WithCancel(ctx)
	s.mu.Unlock()

	sub, err := s.jsAPI.PullSubscribe(s.subject, s.durableName)
	if err != nil {
		return fmt.Errorf("nats: pull subscribe on %s: %w", s.subject, err)
	}

	s.mu.Lock()
	s.sub = sub
	s.mu.Unlock()

	s.logger.Info("starting JetStream pull loop",
		"subject", s.subject,
		"durable", s.durableName,
	)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("subscriber stopped", "reason", ctx.Err())
			return ctx.Err()
		default:
		}

		msgs, err := sub.Fetch(s.batchSize, s.fetchTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Timeout is normal for pull consumers when no messages are available.
			s.logger.Debug("fetch returned", "error", err)
			continue
		}

		for _, msg := range msgs {
			var event stream.Event
			if err := json.Unmarshal(msg.Data(), &event); err != nil {
				s.logger.Error("failed to unmarshal message",
					"subject", msg.Subject(), "error", err,
				)
				if nakErr := msg.Nak(); nakErr != nil {
					s.logger.Error("NAK failed", "error", nakErr)
				}
				continue
			}

			// Store for deferred ACK.
			pending := &pendingMessage{message: msg}
			s.pendingAcks.Store(event.ID, pending)

			if err := handler(ctx, event); err != nil {
				s.logger.Warn("handler returned error",
					"event_id", event.ID, "error", err,
				)
				s.pendingAcks.CompareAndDelete(event.ID, pending)
				if nakErr := msg.Nak(); nakErr != nil {
					s.logger.Error("NAK failed", "event_id", event.ID, "error", nakErr)
				}
				continue
			}
		}
	}
}

// Acknowledge sends ACK for the given event IDs.
func (s *Subscriber) Acknowledge(ctx context.Context, eventIDs ...string) error {
	_ = ctx
	var firstErr error
	for _, eid := range eventIDs {
		rawMsg, ok := s.pendingAcks.Load(eid)
		if !ok {
			s.logger.Warn("no pending ACK for event", "event_id", eid)
			continue
		}
		msg := rawMsg.(*pendingMessage).message
		if err := msg.Ack(); err != nil {
			s.logger.Error("ACK failed", "event_id", eid, "error", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("nats: ACK event %s: %w", eid, err)
			}
		} else {
			s.pendingAcks.CompareAndDelete(eid, rawMsg)
		}
	}
	return firstErr
}

// Close stops the subscriber and unsubscribes from JetStream.
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
	if s.sub != nil {
		if err := s.sub.Unsubscribe(); err != nil {
			s.logger.Error("unsubscribe failed", "error", err)
			return fmt.Errorf("nats: unsubscribe: %w", err)
		}
	}
	s.logger.Info("subscriber closed")
	return nil
}
