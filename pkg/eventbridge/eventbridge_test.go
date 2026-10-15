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
	"sync"
	"testing"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// mockEventBridgeAPI is a mock implementation of EventBridgeAPI for testing.
type mockEventBridgeAPI struct {
	mu             sync.Mutex
	putEventsCalls []*PutEventsInput
	failCount      int
	returnErr      error
}

func (m *mockEventBridgeAPI) PutEvents(_ context.Context, input *PutEventsInput) (*PutEventsOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.putEventsCalls = append(m.putEventsCalls, input)

	if m.returnErr != nil {
		return nil, m.returnErr
	}

	results := make([]PutEventsResultEntry, len(input.Entries))
	failedCount := 0
	for i := range input.Entries {
		if i < m.failCount {
			results[i] = PutEventsResultEntry{
				ErrorCode:    "InternalError",
				ErrorMessage: "simulated failure",
			}
			failedCount++
		} else {
			results[i] = PutEventsResultEntry{
				EventID: fmt.Sprintf("evt-%d", i),
			}
		}
	}

	return &PutEventsOutput{
		FailedEntryCount: failedCount,
		Entries:          results,
	}, nil
}

func newTestEvent(id, tenant string) stream.Event {
	return stream.Event{
		ID:        id,
		Type:      "receipt.created",
		Source:    "maip/test",
		TenantID:  tenant,
		Payload:   json.RawMessage(`{"test": true}`),
		Metadata:  map[string]string{"env": "test"},
		Timestamp: time.Now().UTC(),
	}
}

func TestPublisher_PublishSingleEvent(t *testing.T) {
	mock := &mockEventBridgeAPI{}
	cfg := stream.StreamConfig{
		EventBusName:      "test-bus",
		DetailType:        "TestReceipt",
		EventBridgeSource: "test-source",
	}
	pub := NewPublisher(mock, cfg)

	evt := newTestEvent("evt-1", "tenant-1")
	err := pub.Publish(context.Background(), evt)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(mock.putEventsCalls) != 1 {
		t.Fatalf("expected 1 PutEvents call, got %d", len(mock.putEventsCalls))
	}
	if len(mock.putEventsCalls[0].Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(mock.putEventsCalls[0].Entries))
	}

	entry := mock.putEventsCalls[0].Entries[0]
	if entry.EventBusName != "test-bus" {
		t.Errorf("expected bus name 'test-bus', got %q", entry.EventBusName)
	}
	if entry.DetailType != "TestReceipt" {
		t.Errorf("expected detail type 'TestReceipt', got %q", entry.DetailType)
	}
	if entry.Source != "test-source" {
		t.Errorf("expected source 'test-source', got %q", entry.Source)
	}

	var decoded stream.Event
	if err := json.Unmarshal([]byte(entry.Detail), &decoded); err != nil {
		t.Fatalf("failed to unmarshal detail: %v", err)
	}
	if decoded.ID != "evt-1" {
		t.Errorf("expected event ID 'evt-1', got %q", decoded.ID)
	}
}

func TestPublisher_PublishBatchPartialFailure(t *testing.T) {
	mock := &mockEventBridgeAPI{failCount: 2}
	cfg := stream.StreamConfig{
		EventBusName:      "test-bus",
		EventBridgeSource: "test-source",
	}
	pub := NewPublisher(mock, cfg)

	events := []stream.Event{
		newTestEvent("e1", "t1"),
		newTestEvent("e2", "t1"),
		newTestEvent("e3", "t1"),
		newTestEvent("e4", "t1"),
		newTestEvent("e5", "t1"),
	}

	successes, failures, err := pub.PublishBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("expected no systemic error, got %v", err)
	}
	if successes != 3 {
		t.Errorf("expected 3 successes, got %d", successes)
	}
	if failures != 2 {
		t.Errorf("expected 2 failures, got %d", failures)
	}
}

func TestPublisher_PublishBatchWithApiError(t *testing.T) {
	mock := &mockEventBridgeAPI{returnErr: fmt.Errorf("throttled")}
	cfg := stream.StreamConfig{EventBusName: "test-bus"}
	pub := NewPublisher(mock, cfg)

	events := []stream.Event{newTestEvent("e1", "t1")}
	successes, failures, err := pub.PublishBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("expected no systemic error from PublishBatch, got %v", err)
	}
	if successes != 0 {
		t.Errorf("expected 0 successes, got %d", successes)
	}
	if failures != 1 {
		t.Errorf("expected 1 failure, got %d", failures)
	}
}

func TestPublisher_PublishAfterClose(t *testing.T) {
	mock := &mockEventBridgeAPI{}
	cfg := stream.StreamConfig{EventBusName: "test-bus"}
	pub := NewPublisher(mock, cfg)

	if err := pub.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	err := pub.Publish(context.Background(), newTestEvent("e1", "t1"))
	if err == nil {
		t.Fatal("expected error when publishing after close")
	}
}

func TestPublisher_BatchExceedingMaxBatchSize(t *testing.T) {
	mock := &mockEventBridgeAPI{}
	cfg := stream.StreamConfig{EventBusName: "test-bus"}
	pub := NewPublisher(mock, cfg)

	// Create 25 events, which should be split into 3 PutEvents calls (10+10+5).
	events := make([]stream.Event, 25)
	for i := range events {
		events[i] = newTestEvent(fmt.Sprintf("e-%d", i), "t1")
	}

	err := pub.Publish(context.Background(), events...)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(mock.putEventsCalls) != 3 {
		t.Errorf("expected 3 PutEvents calls, got %d", len(mock.putEventsCalls))
	}
	if len(mock.putEventsCalls[0].Entries) != 10 {
		t.Errorf("first batch should have 10 entries, got %d", len(mock.putEventsCalls[0].Entries))
	}
	if len(mock.putEventsCalls[2].Entries) != 5 {
		t.Errorf("last batch should have 5 entries, got %d", len(mock.putEventsCalls[2].Entries))
	}
}

func TestSubscriber_ReceiveAndAcknowledge(t *testing.T) {
	evt := newTestEvent("sub-evt-1", "t1")
	evtJSON, _ := json.Marshal(evt)
	envelope := eventBridgeSQSEnvelope{
		Source:     "test",
		DetailType: "MAIPReceipt",
		Detail:     json.RawMessage(evtJSON),
		Time:       time.Now(),
	}
	envelopeJSON, _ := json.Marshal(envelope)

	sqsMock := &mockSQSAPI{
		messages: []SQSMessage{
			{
				MessageID:     "sqs-1",
				ReceiptHandle: "handle-1",
				Body:          string(envelopeJSON),
			},
		},
	}

	cfg := stream.StreamConfig{BatchSize: 10}
	sub := NewSubscriber(sqsMock, "https://sqs.test/queue", cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var received []stream.Event
	handler := func(_ context.Context, e stream.Event) error {
		received = append(received, e)
		cancel() // stop after first event
		return nil
	}

	_ = sub.Subscribe(ctx, handler)

	if len(received) != 1 {
		t.Fatalf("expected 1 event, got %d", len(received))
	}
	if received[0].ID != "sub-evt-1" {
		t.Errorf("expected event ID 'sub-evt-1', got %q", received[0].ID)
	}

	// Acknowledge
	err := sub.Acknowledge(context.Background(), "sub-evt-1")
	if err != nil {
		t.Fatalf("acknowledge failed: %v", err)
	}
	if len(sqsMock.deletedHandles) != 1 || sqsMock.deletedHandles[0] != "handle-1" {
		t.Errorf("expected handle-1 to be deleted, got %v", sqsMock.deletedHandles)
	}
}

// mockSQSAPI is a mock implementation of SQSAPI for testing.
type mockSQSAPI struct {
	mu             sync.Mutex
	messages       []SQSMessage
	consumed       bool
	deletedHandles []string
}

func (m *mockSQSAPI) ReceiveMessage(ctx context.Context, _ *ReceiveMessageInput) (*ReceiveMessageOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if m.consumed {
		return &ReceiveMessageOutput{}, nil
	}
	m.consumed = true
	return &ReceiveMessageOutput{Messages: m.messages}, nil
}

func (m *mockSQSAPI) DeleteMessage(_ context.Context, input *DeleteMessageInput) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletedHandles = append(m.deletedHandles, input.ReceiptHandle)
	return nil
}
