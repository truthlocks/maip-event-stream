package middleware

import (
	"context"
	"fmt"
	"github.com/truthlocks/maip-event-stream/pkg/stream"
	"sync"
	"testing"
	"time"
)

func TestDedupeDoesNotRememberFailedPublish(t *testing.T) {
	m := &mockPublisher{publishErr: fmt.Errorf("offline")}
	p := Dedupe(DefaultDedupeConfig())(m)
	e := newTestEvent("retry")
	if err := p.Publish(context.Background(), e); err == nil {
		t.Fatal("missing failure")
	}
	m.publishErr = nil
	if err := p.Publish(context.Background(), e); err != nil || len(m.published) != 1 {
		t.Fatal("failed event was silently suppressed")
	}
}
func TestDedupeScopeConflictAndCapacity(t *testing.T) {
	m := &mockPublisher{}
	p := Dedupe(DedupeConfig{ExpectedItems: 1, FalsePositiveRate: 0.999})(m)
	e := newTestEvent("same")
	if err := p.Publish(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	changed := e
	changed.Type = "different"
	if err := p.Publish(context.Background(), changed); err == nil {
		t.Fatal("conflicting ID silently suppressed")
	}
	other := e
	other.TenantID = "other"
	if err := p.Publish(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(context.Background(), e); err != nil || len(m.published) != 3 {
		t.Fatal("capacity or tenant boundary dropped an event")
	}
}
func TestDedupeConcurrentIdenticalPublish(t *testing.T) {
	m := &mockPublisher{}
	p := Dedupe(DefaultDedupeConfig())(m)
	e := newTestEvent("concurrent")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Publish(context.Background(), e); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(m.published) != 1 {
		t.Fatalf("expected one confirmed publication, got %d", len(m.published))
	}
}

type middleFailure struct{ calls map[string]int }

func (m *middleFailure) Publish(context.Context, ...stream.Event) error { return nil }
func (m *middleFailure) Close() error                                   { return nil }
func (m *middleFailure) PublishBatch(_ context.Context, events []stream.Event) (int, int, error) {
	if len(events) != 1 {
		return 0, len(events), fmt.Errorf("aggregate status cannot identify failed items")
	}
	id := events[0].ID
	m.calls[id]++
	if id == "middle" && m.calls[id] == 1 {
		return 0, 1, nil
	}
	return 1, 0, nil
}
func TestBatchRetriesOnlyKnownFailedIdentity(t *testing.T) {
	m := &middleFailure{calls: map[string]int{}}
	p := Retry(RetryConfig{MaxRetries: 1, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})(m)
	n, f, e := p.PublishBatch(context.Background(), []stream.Event{newTestEvent("first"), newTestEvent("middle"), newTestEvent("last")})
	if e != nil || n != 3 || f != 0 || m.calls["first"] != 1 || m.calls["middle"] != 2 || m.calls["last"] != 1 {
		t.Fatalf("incorrect retries: %v %d %d %v", m.calls, n, f, e)
	}
}
func TestBatchFailureIsNotNilAndNegativeRetriesDoNotSucceed(t *testing.T) {
	m := &mockPublisher{failCount: 1}
	p := Retry(RetryConfig{MaxRetries: -1})(m)
	n, f, e := p.PublishBatch(context.Background(), []stream.Event{newTestEvent("one")})
	if e == nil || n != 0 || f != 1 || m.callCount != 1 {
		t.Fatal("failed batch reported success")
	}
	m.publishErr = fmt.Errorf("offline")
	if e = p.Publish(context.Background(), newTestEvent("two")); e == nil {
		t.Fatal("negative retries skipped failed publication")
	}
}
