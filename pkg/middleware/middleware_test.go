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

package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// mockPublisher is a mock implementation of stream.Publisher for testing.
type mockPublisher struct {
	mu         sync.Mutex
	published  []stream.Event
	failCount  int
	callCount  int
	publishErr error
}

func (m *mockPublisher) Publish(_ context.Context, events ...stream.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCount++
	if m.publishErr != nil {
		return m.publishErr
	}
	m.published = append(m.published, events...)
	return nil
}

func (m *mockPublisher) PublishBatch(_ context.Context, events []stream.Event) (int, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCount++
	if m.publishErr != nil {
		return 0, len(events), m.publishErr
	}

	successes := len(events) - m.failCount
	if successes < 0 {
		successes = 0
	}
	failures := len(events) - successes
	m.published = append(m.published, events[:successes]...)
	return successes, failures, nil
}

func (m *mockPublisher) Close() error {
	return nil
}

func newTestEvent(id string) stream.Event {
	return stream.Event{
		ID:        id,
		Type:      "receipt.created",
		Source:    "maip/test",
		TenantID:  "t1",
		Payload:   json.RawMessage(`{"test": true}`),
		Metadata:  map[string]string{"env": "test"},
		Timestamp: time.Now().UTC(),
	}
}

// --- Retry Tests ---

func TestRetry_SucceedsOnFirstAttempt(t *testing.T) {
	mock := &mockPublisher{}
	pub := Retry(DefaultRetryConfig())(mock)

	err := pub.Publish(context.Background(), newTestEvent("e1"))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if mock.callCount != 1 {
		t.Errorf("expected 1 call, got %d", mock.callCount)
	}
}

func TestRetry_RetriesOnFailure(t *testing.T) {
	callCount := 0
	failingPub := &countingPublisher{
		failUntil: 2,
		callCount: &callCount,
	}

	cfg := RetryConfig{
		MaxRetries:  3,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  10 * time.Millisecond,
	}
	pub := Retry(cfg)(failingPub)

	err := pub.Publish(context.Background(), newTestEvent("e1"))
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if callCount != 3 {
		t.Errorf("expected 3 calls (2 failures + 1 success), got %d", callCount)
	}
}

func TestRetry_ExhaustsRetries(t *testing.T) {
	mock := &mockPublisher{publishErr: fmt.Errorf("permanent failure")}

	cfg := RetryConfig{
		MaxRetries:  2,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
	}
	pub := Retry(cfg)(mock)

	err := pub.Publish(context.Background(), newTestEvent("e1"))
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if mock.callCount != 3 { // initial + 2 retries
		t.Errorf("expected 3 attempts, got %d", mock.callCount)
	}
}

func TestRetry_RespectsContextCancellation(t *testing.T) {
	mock := &mockPublisher{publishErr: fmt.Errorf("fail")}

	cfg := RetryConfig{
		MaxRetries:  10,
		BaseBackoff: time.Second, // long backoff
		MaxBackoff:  time.Minute,
	}
	pub := Retry(cfg)(mock)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := pub.Publish(ctx, newTestEvent("e1"))
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestRetry_BatchPublishRetries(t *testing.T) {
	callCount := 0
	failingPub := &countingPublisher{
		failUntil: 1,
		callCount: &callCount,
		batchMode: true,
	}

	cfg := RetryConfig{
		MaxRetries:  3,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
	}
	pub := Retry(cfg)(failingPub)

	events := []stream.Event{newTestEvent("e1"), newTestEvent("e2")}
	successes, failures, err := pub.PublishBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if successes != 2 {
		t.Errorf("expected 2 successes, got %d", successes)
	}
	if failures != 0 {
		t.Errorf("expected 0 failures, got %d", failures)
	}
}

type countingPublisher struct {
	failUntil int
	callCount *int
	batchMode bool
}

func (p *countingPublisher) Publish(_ context.Context, _ ...stream.Event) error {
	*p.callCount++
	if *p.callCount <= p.failUntil {
		return fmt.Errorf("attempt %d failed", *p.callCount)
	}
	return nil
}

func (p *countingPublisher) PublishBatch(_ context.Context, events []stream.Event) (int, int, error) {
	*p.callCount++
	if *p.callCount <= p.failUntil {
		return 0, len(events), fmt.Errorf("batch attempt %d failed", *p.callCount)
	}
	return len(events), 0, nil
}

func (p *countingPublisher) Close() error { return nil }

// --- Dedupe Tests ---

func TestDedupe_FiltersDuplicates(t *testing.T) {
	mock := &mockPublisher{}
	pub := Dedupe(DefaultDedupeConfig())(mock)

	evt := newTestEvent("dup-1")
	if err := pub.Publish(context.Background(), evt); err != nil {
		t.Fatalf("first publish failed: %v", err)
	}
	// Second publish with same ID should be filtered.
	if err := pub.Publish(context.Background(), evt); err != nil {
		t.Fatalf("second publish failed: %v", err)
	}

	if len(mock.published) != 1 {
		t.Errorf("expected 1 event published (dupe filtered), got %d", len(mock.published))
	}
}

func TestDedupe_AllowsUniqueEvents(t *testing.T) {
	mock := &mockPublisher{}
	pub := Dedupe(DefaultDedupeConfig())(mock)

	for i := 0; i < 100; i++ {
		evt := newTestEvent(fmt.Sprintf("unique-%d", i))
		if err := pub.Publish(context.Background(), evt); err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}

	if len(mock.published) != 100 {
		t.Errorf("expected 100 unique events published, got %d", len(mock.published))
	}
}

func TestDedupe_BatchFiltersDuplicates(t *testing.T) {
	mock := &mockPublisher{}
	pub := Dedupe(DefaultDedupeConfig())(mock)

	events := []stream.Event{
		newTestEvent("b1"),
		newTestEvent("b2"),
		newTestEvent("b1"), // replaced below with the exact event
		newTestEvent("b3"),
	}

	events[2] = events[0]
	successes, _, err := pub.PublishBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("batch publish failed: %v", err)
	}
	// 3 unique events go through, mock has 0 failCount so all succeed.
	if successes != 3 {
		t.Errorf("expected 3 successes, got %d", successes)
	}
}

func TestDedupe_EmptyBatchAfterDedup(t *testing.T) {
	mock := &mockPublisher{}
	pub := Dedupe(DefaultDedupeConfig())(mock)

	evt := newTestEvent("only-one")
	_ = pub.Publish(context.Background(), evt)

	// Publish same event in a batch.
	events := []stream.Event{evt}
	successes, failures, err := pub.PublishBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if successes != 0 || failures != 0 {
		t.Errorf("expected 0/0 for all-dupes batch, got %d/%d", successes, failures)
	}
}

// --- Metrics Tests ---

func TestMetrics_RecordsPublishSuccess(t *testing.T) {
	mock := &mockPublisher{}
	collector := NewMetricsCollector()
	pub := Metrics(collector)(mock)

	events := []stream.Event{newTestEvent("m1"), newTestEvent("m2")}
	if err := pub.Publish(context.Background(), events...); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	if collector.PublishedTotal() != 2 {
		t.Errorf("expected published_total=2, got %d", collector.PublishedTotal())
	}
	if collector.FailedTotal() != 0 {
		t.Errorf("expected failed_total=0, got %d", collector.FailedTotal())
	}
}

func TestMetrics_RecordsPublishFailure(t *testing.T) {
	mock := &mockPublisher{publishErr: fmt.Errorf("fail")}
	collector := NewMetricsCollector()
	pub := Metrics(collector)(mock)

	_ = pub.Publish(context.Background(), newTestEvent("f1"))

	if collector.FailedTotal() != 1 {
		t.Errorf("expected failed_total=1, got %d", collector.FailedTotal())
	}
	if collector.PublishedTotal() != 0 {
		t.Errorf("expected published_total=0, got %d", collector.PublishedTotal())
	}
}

func TestMetrics_BatchMetrics(t *testing.T) {
	mock := &mockPublisher{failCount: 1}
	collector := NewMetricsCollector()
	pub := Metrics(collector)(mock)

	events := []stream.Event{newTestEvent("b1"), newTestEvent("b2"), newTestEvent("b3")}
	successes, failures, err := pub.PublishBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = successes
	_ = failures

	if collector.BatchTotal() != 1 {
		t.Errorf("expected batch_total=1, got %d", collector.BatchTotal())
	}
	if collector.PublishedTotal() != 2 {
		t.Errorf("expected published_total=2, got %d", collector.PublishedTotal())
	}
	if collector.FailedTotal() != 1 {
		t.Errorf("expected failed_total=1, got %d", collector.FailedTotal())
	}
}

func TestMetrics_Snapshot(t *testing.T) {
	collector := NewMetricsCollector()
	mock := &mockPublisher{}
	pub := Metrics(collector)(mock)

	_ = pub.Publish(context.Background(), newTestEvent("s1"))

	snap := collector.Snapshot()
	if snap.PublishedTotal != 1 {
		t.Errorf("snapshot published_total mismatch: got %d", snap.PublishedTotal)
	}
	// Latency may be zero on fast machines (sub-nanosecond mock), so we only
	// check that it is non-negative.
	if snap.AverageLatencyMs < 0 {
		t.Error("expected non-negative average latency")
	}
}

func TestMetrics_AverageLatencyZeroWhenNoCalls(t *testing.T) {
	collector := NewMetricsCollector()
	if collector.AverageLatencyMs() != 0 {
		t.Errorf("expected 0 average latency when no calls, got %f", collector.AverageLatencyMs())
	}
}

// --- ApplyMiddleware Tests ---

func TestApplyMiddleware_ChainsCorrectly(t *testing.T) {
	mock := &mockPublisher{}
	collector := NewMetricsCollector()

	// Chain order: outermost first. Metrics wraps Dedupe wraps mock.
	// Publish -> Metrics -> Dedupe -> mock
	pub := stream.ApplyMiddleware(mock,
		Metrics(collector),
		Dedupe(DefaultDedupeConfig()),
	)

	// Publish same event twice; dedupe should filter the second.
	evt := newTestEvent("chain-1")
	_ = pub.Publish(context.Background(), evt)
	_ = pub.Publish(context.Background(), evt)

	if len(mock.published) != 1 {
		t.Errorf("expected 1 event (deduped), got %d", len(mock.published))
	}
	// Metrics is outermost, so it sees both publish calls. The first succeeds
	// (1 event), the second also "succeeds" (dedupe returns nil with 0 events
	// but metrics records len(events) passed to it).
	if collector.PublishedTotal() != 2 {
		t.Errorf("expected metrics to see 2 publishes, got %d", collector.PublishedTotal())
	}
}
