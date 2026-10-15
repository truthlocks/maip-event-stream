package kinesis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/truthlocks/maip-event-stream/pkg/stream"
	"strings"
	"testing"
	"time"
)

func TestHandlerFailureCannotSkipToLaterCheckpoint(t *testing.T) {
	data, _ := json.Marshal(newTestEvent("qa", "tenant"))
	r := &mockKinesisReaderAPI{shards: []ShardInfo{{ShardID: "one"}, {ShardID: "idle"}}, records: map[string][]KinesisRecord{"one": {{SequenceNumber: "1", Data: data}, {SequenceNumber: "2", Data: data}}}}
	cp := NewInMemoryCheckpointer()
	s := NewSubscriber(r, cp, stream.StreamConfig{FlushInterval: time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := s.Subscribe(ctx, func(context.Context, stream.Event) error { return fmt.Errorf("handler rejected") })
	if err == nil || !strings.Contains(err.Error(), "handler rejected") || ctx.Err() != nil {
		t.Fatalf("failed shard did not stop promptly: %v", err)
	}
	seq, _ := cp.GetCheckpoint(context.Background(), "one")
	if seq != "" {
		t.Fatal("checkpoint skipped failed event")
	}
}

type resumeReader struct {
	mockKinesisReaderAPI
	sequence string
}

func (r *resumeReader) GetShardIteratorAfter(_ context.Context, _, shard, sequence string) (string, error) {
	r.sequence = sequence
	return "iterator-" + shard, nil
}
func TestResumeSuppliesExactCheckpoint(t *testing.T) {
	cp := NewInMemoryCheckpointer()
	cp.SetCheckpoint(context.Background(), "one", "42")
	r := &resumeReader{}
	s := NewSubscriber(r, cp, stream.StreamConfig{FlushInterval: time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	_ = s.consumeShard(ctx, "one", func(context.Context, stream.Event) error { return nil })
	if r.sequence != "42" {
		t.Fatal("checkpoint sequence not supplied")
	}
	legacy := NewSubscriber(&mockKinesisReaderAPI{}, cp, stream.StreamConfig{})
	if err := legacy.consumeShard(context.Background(), "one", func(context.Context, stream.Event) error { return nil }); err == nil || !strings.Contains(err.Error(), "cannot resume") {
		t.Fatal("legacy client silently ignored checkpoint")
	}
}

type failedCheckpointer struct{}

func (failedCheckpointer) GetCheckpoint(context.Context, string) (string, error) { return "", nil }
func (failedCheckpointer) SetCheckpoint(context.Context, string, string) error {
	return fmt.Errorf("storage down")
}
func TestCheckpointFailureStopsBeforeNextRecord(t *testing.T) {
	data, _ := json.Marshal(newTestEvent("qa", "tenant"))
	r := &mockKinesisReaderAPI{records: map[string][]KinesisRecord{"one": {{SequenceNumber: "1", Data: data}, {SequenceNumber: "2", Data: data}}}}
	s := NewSubscriber(r, failedCheckpointer{}, stream.StreamConfig{FlushInterval: time.Millisecond})
	seen := 0
	err := s.consumeShard(context.Background(), "one", func(context.Context, stream.Event) error { seen++; return nil })
	if err == nil || seen != 1 {
		t.Fatal("advanced beyond failed checkpoint")
	}
}
