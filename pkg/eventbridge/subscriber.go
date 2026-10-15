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

package eventbridge

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// SQSAPI abstracts the subset of the AWS SQS client used by the EventBridge
// subscriber. EventBridge delivers events to an SQS queue via a rule; this
// subscriber polls that queue.
type SQSAPI interface {
	ReceiveMessage(ctx context.Context, input *ReceiveMessageInput) (*ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, input *DeleteMessageInput) error
}

// ReceiveMessageInput mirrors the AWS SDK ReceiveMessageInput.
type ReceiveMessageInput struct {
	QueueURL            string
	MaxNumberOfMessages int
	WaitTimeSeconds     int
}

// ReceiveMessageOutput mirrors the AWS SDK ReceiveMessageOutput.
type ReceiveMessageOutput struct {
	Messages []SQSMessage
}

// SQSMessage mirrors a single SQS message.
type SQSMessage struct {
	MessageID     string
	ReceiptHandle string
	Body          string
}

// DeleteMessageInput mirrors the AWS SDK DeleteMessageInput.
type DeleteMessageInput struct {
	QueueURL      string
	ReceiptHandle string
}

// eventBridgeSQSEnvelope is the JSON envelope that EventBridge puts around
// the detail payload when delivering to SQS.
type eventBridgeSQSEnvelope struct {
	Source     string          `json:"source"`
	DetailType string          `json:"detail-type"`
	Detail     json.RawMessage `json:"detail"`
	Time       time.Time       `json:"time"`
}

// Subscriber polls an SQS queue that receives events from an EventBridge
// rule. It unwraps the EventBridge envelope and delivers the inner Event
// to the handler.
type Subscriber struct {
	sqsClient SQSAPI
	queueURL  string
	batchSize int
	waitTime  int
	logger    *slog.Logger

	mu     sync.Mutex
	closed bool
	cancel context.CancelFunc

	// pendingAcks maps event ID to SQS receipt handle for deferred acknowledgement.
	pendingAcks sync.Map
}

// NewSubscriber creates an EventBridge subscriber that polls the given SQS
// queue URL. Events arriving on the queue are expected to have the standard
// EventBridge envelope wrapping an Event JSON detail.
func NewSubscriber(sqsClient SQSAPI, queueURL string, cfg stream.StreamConfig) *Subscriber {
	batchSize := cfg.BatchSize
	if batchSize <= 0 || batchSize > 10 {
		batchSize = 10
	}

	return &Subscriber{
		sqsClient: sqsClient,
		queueURL:  queueURL,
		batchSize: batchSize,
		waitTime:  20,
		logger:    slog.Default().With("component", "eventbridge-subscriber"),
	}
}

// Subscribe begins polling the SQS queue and invoking handler for each
// received event. It blocks until ctx is cancelled or a fatal error occurs.
func (s *Subscriber) Subscribe(ctx context.Context, handler stream.EventHandler) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("eventbridge: subscriber is closed")
	}
	ctx, s.cancel = context.WithCancel(ctx)
	s.mu.Unlock()

	s.logger.Info("starting SQS poll loop", "queue_url", s.queueURL)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("subscriber stopped", "reason", ctx.Err())
			return ctx.Err()
		default:
		}

		output, err := s.sqsClient.ReceiveMessage(ctx, &ReceiveMessageInput{
			QueueURL:            s.queueURL,
			MaxNumberOfMessages: s.batchSize,
			WaitTimeSeconds:     s.waitTime,
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			s.logger.Error("ReceiveMessage failed", "error", err)
			continue
		}

		for _, msg := range output.Messages {
			var envelope eventBridgeSQSEnvelope
			if err := json.Unmarshal([]byte(msg.Body), &envelope); err != nil {
				s.logger.Error("failed to unmarshal SQS message envelope",
					"message_id", msg.MessageID, "error", err,
				)
				continue
			}

			var event stream.Event
			if err := json.Unmarshal(envelope.Detail, &event); err != nil {
				s.logger.Error("failed to unmarshal event detail",
					"message_id", msg.MessageID, "error", err,
				)
				continue
			}

			// Store the receipt handle for later ACK.
			s.pendingAcks.Store(event.ID, msg.ReceiptHandle)

			if err := handler(ctx, event); err != nil {
				s.logger.Warn("handler returned error",
					"event_id", event.ID, "error", err,
				)
				// Do not delete the message; it will become visible again after
				// the visibility timeout expires.
				s.pendingAcks.Delete(event.ID)
				continue
			}
		}
	}
}

// Acknowledge deletes the SQS messages corresponding to the given event IDs,
// removing them from the queue permanently.
func (s *Subscriber) Acknowledge(ctx context.Context, eventIDs ...string) error {
	var firstErr error
	for _, id := range eventIDs {
		handle, ok := s.pendingAcks.Load(id)
		if !ok {
			s.logger.Warn("no pending ACK for event", "event_id", id)
			continue
		}

		if err := s.sqsClient.DeleteMessage(ctx, &DeleteMessageInput{
			QueueURL:      s.queueURL,
			ReceiptHandle: handle.(string),
		}); err != nil {
			s.logger.Error("failed to delete SQS message",
				"event_id", id, "error", err,
			)
			if firstErr == nil {
				firstErr = fmt.Errorf("eventbridge: delete message for event %s: %w", id, err)
			}
		} else {
			s.pendingAcks.CompareAndDelete(id, handle)
		}
	}
	return firstErr
}

// Close stops the subscriber and releases resources.
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
