package nats

import (
	"context"
	"github.com/truthlocks/maip-event-stream/pkg/stream"
	"strings"
	"testing"
)

func TestSubjectTokensCannotAliasOtherTenants(t *testing.T) {
	values := []string{"org.a", "org-a", "org a", "org*a", "orga", "org>a", "org%2Ea", "org\ta", "org\na", "组织", "unknown", "%", "*", ">"}
	seen := map[string]string{}
	for _, v := range values {
		token := SubjectToken(v)
		if strings.ContainsAny(token, ".*> \t\r\n") {
			t.Fatalf("invalid token %q", token)
		}
		if before, ok := seen[token]; ok {
			t.Fatalf("%q aliases %q", before, v)
		}
		seen[token] = v
	}
	if SubjectToken("tenant-123") != "tenant-123" {
		t.Fatal("safe identifiers changed")
	}
	if SubjectToken("receipt.created") != "receipt%2Ecreated" {
		t.Fatal("incorrect exact subscription token")
	}
}

func TestPublisherRejectsInvalidBaseAndCancelledRequests(t *testing.T) {
	for _, base := range []string{"maip.*.receipts", "maip..receipts", "maip.\treceipts", ".maip", "maip.>.*"} {
		client := &mockJetStreamAPI{}
		pub := NewPublisher(client, stream.StreamConfig{NatsSubject: base})
		if pub.Publish(context.Background(), newTestEvent("id", "tenant", "receipt.created")) == nil || client.callCount != 0 {
			t.Fatalf("accepted invalid base %q", base)
		}
	}
	client := &mockJetStreamAPI{}
	pub := NewPublisher(client, stream.StreamConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	events := []stream.Event{newTestEvent("a", "t", "created"), newTestEvent("b", "t", "created")}
	if pub.Publish(ctx, events...) == nil {
		t.Fatal("ignored cancellation")
	}
	success, failed, err := pub.PublishBatch(ctx, events)
	if err == nil || success != 0 || failed != 2 || client.callCount != 0 {
		t.Fatal("cancelled batch published")
	}
}

type missingAckClient struct {
	mockJetStreamAPI
	ack *PubAck
}

func (m *missingAckClient) Publish(string, []byte) (*PubAck, error) { return m.ack, nil }
func TestPublisherRequiresServerAcknowledgement(t *testing.T) {
	for _, ack := range []*PubAck{nil, {}, {Stream: "s"}, {Sequence: 1}} {
		pub := NewPublisher(&missingAckClient{ack: ack}, stream.StreamConfig{})
		event := newTestEvent("id", "tenant", "created")
		if pub.Publish(context.Background(), event) == nil {
			t.Fatal("unconfirmed event succeeded")
		}
		success, failed, _ := pub.PublishBatch(context.Background(), []stream.Event{event})
		if success != 0 || failed != 1 {
			t.Fatal("unconfirmed batch counted as success")
		}
	}
}

func TestBatchAfterCloseReturnsSystemicError(t *testing.T) {
	client := &mockJetStreamAPI{}
	pub := NewPublisher(client, stream.StreamConfig{})
	if err := pub.Close(); err != nil {
		t.Fatal(err)
	}
	for _, events := range [][]stream.Event{nil, {newTestEvent("id", "tenant", "created")}} {
		success, failed, err := pub.PublishBatch(context.Background(), events)
		if err == nil || success != 0 || failed != len(events) || client.callCount != 0 {
			t.Fatal("closed publisher reported success")
		}
	}
}
