package nats

import (
	"context"
	"fmt"
	"github.com/truthlocks/maip-event-stream/pkg/stream"
	"testing"
)

type flakyAck struct {
	mockJetStreamMsg
	calls int
}

func (m *flakyAck) Ack() error {
	m.calls++
	if m.calls == 1 {
		return fmt.Errorf("temporary failure")
	}
	return nil
}
func TestFailedAckCanBeRetried(t *testing.T) {
	m := &flakyAck{}
	s := NewSubscriber(&mockJetStreamSubscribeAPI{}, stream.StreamConfig{})
	s.pendingAcks.Store("qa", &pendingMessage{message: m})
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
