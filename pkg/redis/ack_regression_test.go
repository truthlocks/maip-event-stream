package redis

import (
	"context"
	"fmt"
	"github.com/truthlocks/maip-event-stream/pkg/stream"
	"testing"
)

type flakyAck struct {
	mockRedisStreamAPI
	calls int
}

func (m *flakyAck) XAck(_ context.Context, _, _ string, ids ...string) (int64, error) {
	m.calls++
	if m.calls == 1 {
		return 0, fmt.Errorf("temporary failure")
	}
	return int64(len(ids)), nil
}
func TestFailedAckCanBeRetried(t *testing.T) {
	m := &flakyAck{}
	s := NewSubscriber(m, stream.StreamConfig{})
	s.pendingAcks.Store("qa", "1-0")
	if s.Acknowledge(context.Background(), "qa") == nil {
		t.Fatal("failure lost")
	}
	if s.Acknowledge(context.Background(), "qa") != nil || m.calls != 2 {
		t.Fatal("pending acknowledgement lost")
	}
	if _, ok := s.pendingAcks.Load("qa"); ok {
		t.Fatal("confirmed acknowledgement retained")
	}
}
