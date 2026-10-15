package redis

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

type startupClient struct {
	mockRedisStreamAPI
	groupErr error
	reads    atomic.Int32
	entered  chan struct{}
}

func (m *startupClient) XGroupCreateMkStream(context.Context, string, string, string) error {
	return m.groupErr
}
func (m *startupClient) XReadGroup(ctx context.Context, _ *XReadGroupArgs) ([]XStream, error) {
	if m.reads.Add(1) == 1 && m.entered != nil {
		close(m.entered)
	}
	return nil, errors.New("connection unavailable")
}
func TestGroupCreationFailureStopsConsumption(t *testing.T) {
	for _, msg := range []string{"NOAUTH Authentication required", "connection refused", "unexpected BUSYGROUP error"} {
		t.Run(msg, func(t *testing.T) {
			cause := errors.New(msg)
			client := &startupClient{groupErr: cause}
			sub := NewSubscriber(client, stream.StreamConfig{})
			err := sub.Subscribe(context.Background(), func(context.Context, stream.Event) error { return nil })
			if !errors.Is(err, cause) || client.reads.Load() != 0 {
				t.Fatalf("startup failure ignored: %v", err)
			}
		})
	}
}
func TestReadFailureBackoffAndConcurrentLifecycle(t *testing.T) {
	client := &startupClient{groupErr: errors.New("BUSYGROUP Consumer Group name already exists"), entered: make(chan struct{})}
	sub := NewSubscriber(client, stream.StreamConfig{})
	done := make(chan error, 1)
	handler := func(context.Context, stream.Event) error { return nil }
	go func() { done <- sub.Subscribe(context.Background(), handler) }()
	select {
	case <-client.entered:
	case <-time.After(time.Second):
		t.Fatal("did not start")
	}
	if err := sub.Subscribe(context.Background(), handler); err == nil {
		t.Fatal("concurrent subscription accepted")
	}
	time.Sleep(50 * time.Millisecond)
	if got := client.reads.Load(); got != 1 {
		t.Fatalf("read errors busy-looped: %d", got)
	}
	sub.Close()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not interrupt backoff")
	}
	if err := sub.Subscribe(context.Background(), handler); err == nil {
		t.Fatal("closed subscriber restarted")
	}
}
func TestStartupCanRetryAfterFailure(t *testing.T) {
	client := &startupClient{groupErr: errors.New("NOAUTH")}
	sub := NewSubscriber(client, stream.StreamConfig{})
	handler := func(context.Context, stream.Event) error { return nil }
	if sub.Subscribe(context.Background(), nil) == nil {
		t.Fatal("nil handler accepted")
	}
	if sub.Subscribe(context.Background(), handler) == nil {
		t.Fatal("startup failure ignored")
	}
	client.groupErr = nil
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sub.Subscribe(ctx, handler); !errors.Is(err, context.Canceled) {
		t.Fatalf("retry blocked: %v", err)
	}
}
