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
	"sync"
	"testing"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// mockRedisStreamAPI is a mock implementation of RedisStreamAPI for testing.
type mockRedisStreamAPI struct {
	mu       sync.Mutex
	messages []XAddArgs
	streams  []XMessage
	acked    []string
	addErr   error
	readErr  error
	consumed bool
}

func (m *mockRedisStreamAPI) XAdd(_ context.Context, args *XAddArgs) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.addErr != nil {
		return "", m.addErr
	}

	m.messages = append(m.messages, *args)
	return fmt.Sprintf("%d-0", len(m.messages)), nil
}

func (m *mockRedisStreamAPI) XReadGroup(ctx context.Context, _ *XReadGroupArgs) ([]XStream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if m.readErr != nil {
		return nil, m.readErr
	}

	if m.consumed || len(m.streams) == 0 {
		return nil, fmt.Errorf("no messages")
	}
	m.consumed = true
	return []XStream{
		{Stream: "maip:receipts", Messages: m.streams},
	}, nil
}

func (m *mockRedisStreamAPI) XAck(_ context.Context, _, _ string, ids ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = append(m.acked, ids...)
	return int64(len(ids)), nil
}

func (m *mockRedisStreamAPI) XGroupCreateMkStream(_ context.Context, _, _, _ string) error {
	return nil
}

func (m *mockRedisStreamAPI) Close() error {
	return nil
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
	mock := &mockRedisStreamAPI{}
	cfg := stream.StreamConfig{
		RedisStreamKey: "test:stream",
		RedisMaxLen:    50000,
	}
	pub := NewPublisher(mock, cfg)

	evt := newTestEvent("redis-evt-1", "tenant-1")
	err := pub.Publish(context.Background(), evt)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(mock.messages) != 1 {
		t.Fatalf("expected 1 XADD call, got %d", len(mock.messages))
	}

	msg := mock.messages[0]
	if msg.Stream != "test:stream" {
		t.Errorf("expected stream 'test:stream', got %q", msg.Stream)
	}
	if msg.MaxLen != 50000 {
		t.Errorf("expected maxlen 50000, got %d", msg.MaxLen)
	}
	if !msg.Approx {
		t.Error("expected approximate trimming to be true")
	}
	if msg.Values["event_id"] != "redis-evt-1" {
		t.Errorf("expected event_id 'redis-evt-1', got %v", msg.Values["event_id"])
	}
}

func TestPublisher_PublishBatchPartialFailure(t *testing.T) {
	callCount := 0
	mock := &mockRedisStreamAPI{}
	// Override to fail on second event.
	origXAdd := mock.XAdd
	_ = origXAdd
	failOnSecond := &failingRedisAPI{failOnIndex: 1}

	cfg := stream.StreamConfig{RedisStreamKey: "test:stream", RedisMaxLen: 100}
	pub := NewPublisher(failOnSecond, cfg)

	events := []stream.Event{
		newTestEvent("e1", "t1"),
		newTestEvent("e2", "t1"),
		newTestEvent("e3", "t1"),
	}

	successes, failures, err := pub.PublishBatch(context.Background(), events)
	_ = callCount
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

// failingRedisAPI fails on a specific XADD call index.
type failingRedisAPI struct {
	mu          sync.Mutex
	callIdx     int
	failOnIndex int
}

func (f *failingRedisAPI) XAdd(_ context.Context, args *XAddArgs) (string, error) {
	f.mu.Lock()
	idx := f.callIdx
	f.callIdx++
	f.mu.Unlock()

	if idx == f.failOnIndex {
		return "", fmt.Errorf("simulated redis error")
	}
	return fmt.Sprintf("%d-0", idx), nil
}

func (f *failingRedisAPI) XReadGroup(_ context.Context, _ *XReadGroupArgs) ([]XStream, error) {
	return nil, nil
}

func (f *failingRedisAPI) XAck(_ context.Context, _, _ string, _ ...string) (int64, error) {
	return 0, nil
}

func (f *failingRedisAPI) XGroupCreateMkStream(_ context.Context, _, _, _ string) error {
	return nil
}

func (f *failingRedisAPI) Close() error {
	return nil
}

func TestPublisher_PublishAfterClose(t *testing.T) {
	mock := &mockRedisStreamAPI{}
	cfg := stream.StreamConfig{RedisStreamKey: "test:stream"}
	pub := NewPublisher(mock, cfg)

	if err := pub.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	err := pub.Publish(context.Background(), newTestEvent("e1", "t1"))
	if err == nil {
		t.Fatal("expected error when publishing after close")
	}
}

func TestPublisher_PublishWithXAddError(t *testing.T) {
	mock := &mockRedisStreamAPI{addErr: fmt.Errorf("connection refused")}
	cfg := stream.StreamConfig{RedisStreamKey: "test:stream"}
	pub := NewPublisher(mock, cfg)

	err := pub.Publish(context.Background(), newTestEvent("e1", "t1"))
	if err == nil {
		t.Fatal("expected error from XADD failure")
	}
}

func TestSubscriber_ReceiveAndAcknowledge(t *testing.T) {
	evt := newTestEvent("redis-sub-1", "t1")
	evtJSON, _ := json.Marshal(evt)

	mock := &mockRedisStreamAPI{
		streams: []XMessage{
			{
				ID: "1234-0",
				Values: map[string]interface{}{
					"event_id":  "redis-sub-1",
					"type":      "receipt.created",
					"tenant_id": "t1",
					"data":      string(evtJSON),
				},
			},
		},
	}

	cfg := stream.StreamConfig{
		RedisStreamKey:     "test:stream",
		RedisConsumerGroup: "test-group",
		RedisConsumerName:  "test-consumer",
		BatchSize:          10,
	}
	sub := NewSubscriber(mock, cfg)

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
	if received[0].ID != "redis-sub-1" {
		t.Errorf("expected event ID 'redis-sub-1', got %q", received[0].ID)
	}

	// Acknowledge.
	err := sub.Acknowledge(context.Background(), "redis-sub-1")
	if err != nil {
		t.Fatalf("acknowledge failed: %v", err)
	}

	if len(mock.acked) != 1 || mock.acked[0] != "1234-0" {
		t.Errorf("expected stream ID '1234-0' to be acked, got %v", mock.acked)
	}
}
