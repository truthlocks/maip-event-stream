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
	"sync"
	"testing"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// mockJetStreamAPI is a mock implementation of JetStreamAPI for testing.
type mockJetStreamAPI struct {
	mu        sync.Mutex
	published []publishedMsg
	failOn    int // fail on the Nth Publish call (1-indexed); 0 = never fail
	callCount int
}

type publishedMsg struct {
	Subject string
	Data    []byte
}

func (m *mockJetStreamAPI) Publish(subject string, data []byte) (*PubAck, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCount++
	if m.failOn > 0 && m.callCount == m.failOn {
		return nil, fmt.Errorf("simulated NATS error")
	}

	m.published = append(m.published, publishedMsg{Subject: subject, Data: data})
	return &PubAck{
		Stream:   "MAIP_RECEIPTS",
		Sequence: uint64(m.callCount),
	}, nil
}

func (m *mockJetStreamAPI) PublishAsync(subject string, data []byte) error {
	_, err := m.Publish(subject, data)
	return err
}

func (m *mockJetStreamAPI) Close() error {
	return nil
}

func newTestEvent(id, tenant, eventType string) stream.Event {
	return stream.Event{
		ID:        id,
		Type:      eventType,
		Source:    "maip/test",
		TenantID:  tenant,
		Payload:   json.RawMessage(`{"test": true}`),
		Metadata:  map[string]string{"env": "test"},
		Timestamp: time.Now().UTC(),
	}
}

func TestPublisher_PublishSingleEvent(t *testing.T) {
	mock := &mockJetStreamAPI{}
	cfg := stream.StreamConfig{NatsSubject: "maip.receipts.>"}
	pub := NewPublisher(mock, cfg)

	evt := newTestEvent("nats-1", "tenant-abc", "receipt.created")
	err := pub.Publish(context.Background(), evt)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(mock.published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(mock.published))
	}

	msg := mock.published[0]
	expectedSubject := "maip.receipts.tenant-abc.receipt%2Ecreated"
	if msg.Subject != expectedSubject {
		t.Errorf("expected subject %q, got %q", expectedSubject, msg.Subject)
	}

	var decoded stream.Event
	if err := json.Unmarshal(msg.Data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal published data: %v", err)
	}
	if decoded.ID != "nats-1" {
		t.Errorf("expected event ID 'nats-1', got %q", decoded.ID)
	}
}

func TestPublisher_SubjectHierarchy(t *testing.T) {
	tests := []struct {
		name     string
		tenant   string
		evtType  string
		expected string
	}{
		{
			name:     "standard subject",
			tenant:   "org-123",
			evtType:  "receipt.verified",
			expected: "maip.receipts.org-123.receipt%2Everified",
		},
		{
			name:     "empty tenant",
			tenant:   "",
			evtType:  "receipt.created",
			expected: "maip.receipts.unknown.receipt%2Ecreated",
		},
		{
			name:     "empty type",
			tenant:   "org-1",
			evtType:  "",
			expected: "maip.receipts.org-1.event",
		},
		{
			name:     "tenant with dots",
			tenant:   "org.with.dots",
			evtType:  "receipt.created",
			expected: "maip.receipts.org%2Ewith%2Edots.receipt%2Ecreated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockJetStreamAPI{}
			cfg := stream.StreamConfig{NatsSubject: "maip.receipts.>"}
			pub := NewPublisher(mock, cfg)

			evt := newTestEvent("id-1", tt.tenant, tt.evtType)
			err := pub.Publish(context.Background(), evt)
			if tt.tenant == "" || tt.evtType == "" {
				if err == nil || len(mock.published) != 0 {
					t.Fatal("missing scope must fail before publishing")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if mock.published[0].Subject != tt.expected {
				t.Errorf("expected subject %q, got %q", tt.expected, mock.published[0].Subject)
			}
		})
	}
}

func TestPublisher_PublishBatchPartialFailure(t *testing.T) {
	mock := &mockJetStreamAPI{failOn: 2}
	cfg := stream.StreamConfig{NatsSubject: "maip.receipts.>"}
	pub := NewPublisher(mock, cfg)

	events := []stream.Event{
		newTestEvent("e1", "t1", "receipt.created"),
		newTestEvent("e2", "t1", "receipt.created"),
		newTestEvent("e3", "t1", "receipt.created"),
	}

	successes, failures, err := pub.PublishBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("expected no systemic error, got %v", err)
	}
	if successes != 2 {
		t.Errorf("expected 2 successes, got %d", successes)
	}
	if failures != 1 {
		t.Errorf("expected 1 failure, got %d", failures)
	}
}

func TestPublisher_PublishAfterClose(t *testing.T) {
	mock := &mockJetStreamAPI{}
	cfg := stream.StreamConfig{NatsSubject: "maip.receipts.>"}
	pub := NewPublisher(mock, cfg)

	if err := pub.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	err := pub.Publish(context.Background(), newTestEvent("e1", "t1", "receipt.created"))
	if err == nil {
		t.Fatal("expected error when publishing after close")
	}
}

func TestSubscriber_ReceiveAndAcknowledge(t *testing.T) {
	evt := newTestEvent("nats-sub-1", "t1", "receipt.created")
	evtJSON, _ := json.Marshal(evt)

	mockMsg := &mockJetStreamMsg{
		data:    evtJSON,
		subject: "maip.receipts.t1.receipt%2Ecreated",
	}

	mockSubAPI := &mockJetStreamSubscribeAPI{
		sub: &mockPullSubscription{
			msgs: []JetStreamMsg{mockMsg},
		},
	}

	cfg := stream.StreamConfig{
		NatsSubject:     "maip.receipts.>",
		NatsDurableName: "test-durable",
		BatchSize:       10,
		FlushInterval:   50 * time.Millisecond,
	}
	sub := NewSubscriber(mockSubAPI, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var received []stream.Event
	handler := func(_ context.Context, e stream.Event) error {
		received = append(received, e)
		cancel()
		return nil
	}

	_ = sub.Subscribe(ctx, handler)

	if len(received) != 1 {
		t.Fatalf("expected 1 event, got %d", len(received))
	}
	if received[0].ID != "nats-sub-1" {
		t.Errorf("expected event ID 'nats-sub-1', got %q", received[0].ID)
	}

	// Acknowledge.
	err := sub.Acknowledge(context.Background(), "nats-sub-1")
	if err != nil {
		t.Fatalf("acknowledge failed: %v", err)
	}
	if !mockMsg.acked {
		t.Error("expected message to be ACKed")
	}
}

// --- Mocks for subscriber testing ---

type mockJetStreamSubscribeAPI struct {
	sub PullSubscription
}

func (m *mockJetStreamSubscribeAPI) PullSubscribe(_, _ string) (PullSubscription, error) {
	return m.sub, nil
}

type mockPullSubscription struct {
	mu       sync.Mutex
	msgs     []JetStreamMsg
	consumed bool
}

func (m *mockPullSubscription) Fetch(_ int, _ time.Duration) ([]JetStreamMsg, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.consumed || len(m.msgs) == 0 {
		return nil, fmt.Errorf("timeout")
	}
	m.consumed = true
	return m.msgs, nil
}

func (m *mockPullSubscription) Unsubscribe() error {
	return nil
}

type mockJetStreamMsg struct {
	data    []byte
	subject string
	acked   bool
	naked   bool
}

func (m *mockJetStreamMsg) Data() []byte    { return m.data }
func (m *mockJetStreamMsg) Subject() string { return m.subject }
func (m *mockJetStreamMsg) Ack() error      { m.acked = true; return nil }
func (m *mockJetStreamMsg) Nak() error      { m.naked = true; return nil }
func (m *mockJetStreamMsg) Metadata() (*MsgMetadata, error) {
	return &MsgMetadata{
		Sequence:  1,
		Stream:    "MAIP_RECEIPTS",
		Consumer:  "test",
		Timestamp: time.Now(),
	}, nil
}
