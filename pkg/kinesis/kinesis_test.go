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

package kinesis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// mockKinesisAPI is a mock implementation of KinesisAPI for testing.
type mockKinesisAPI struct {
	mu              sync.Mutex
	putRecordsCalls []*PutRecordsInput
	failCount       int
	returnErr       error
}

func (m *mockKinesisAPI) PutRecords(_ context.Context, input *PutRecordsInput) (*PutRecordsOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.putRecordsCalls = append(m.putRecordsCalls, input)

	if m.returnErr != nil {
		return nil, m.returnErr
	}

	results := make([]PutRecordsResultEntry, len(input.Records))
	failedCount := 0
	for i := range input.Records {
		if i < m.failCount {
			results[i] = PutRecordsResultEntry{
				ErrorCode:    "ProvisionedThroughputExceededException",
				ErrorMessage: "rate exceeded",
			}
			failedCount++
		} else {
			results[i] = PutRecordsResultEntry{
				SequenceNumber: fmt.Sprintf("seq-%d", i),
				ShardID:        "shard-0",
			}
		}
	}

	return &PutRecordsOutput{
		FailedRecordCount: failedCount,
		Records:           results,
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
	mock := &mockKinesisAPI{}
	cfg := stream.StreamConfig{KinesisStreamName: "test-stream", PartitionKeyField: "tenant_id"}
	pub := NewPublisher(mock, cfg)

	evt := newTestEvent("evt-1", "tenant-abc")
	err := pub.Publish(context.Background(), evt)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(mock.putRecordsCalls) != 1 {
		t.Fatalf("expected 1 PutRecords call, got %d", len(mock.putRecordsCalls))
	}

	call := mock.putRecordsCalls[0]
	if call.StreamName != "test-stream" {
		t.Errorf("expected stream 'test-stream', got %q", call.StreamName)
	}
	if len(call.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(call.Records))
	}
	if call.Records[0].PartitionKey != "tenant-abc" {
		t.Errorf("expected partition key 'tenant-abc', got %q", call.Records[0].PartitionKey)
	}

	var decoded stream.Event
	if err := json.Unmarshal(call.Records[0].Data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal record data: %v", err)
	}
	if decoded.ID != "evt-1" {
		t.Errorf("expected event ID 'evt-1', got %q", decoded.ID)
	}
}

func TestPublisher_PartitionKeyById(t *testing.T) {
	mock := &mockKinesisAPI{}
	cfg := stream.StreamConfig{
		KinesisStreamName: "test-stream",
		PartitionKeyField: "id",
	}
	pub := NewPublisher(mock, cfg)

	evt := newTestEvent("unique-id-123", "tenant-x")
	if err := pub.Publish(context.Background(), evt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.putRecordsCalls[0].Records[0].PartitionKey != "unique-id-123" {
		t.Errorf("expected partition key from ID field, got %q",
			mock.putRecordsCalls[0].Records[0].PartitionKey)
	}
}

func TestPublisher_PublishBatchPartialFailure(t *testing.T) {
	mock := &mockKinesisAPI{failCount: 3}
	cfg := stream.StreamConfig{KinesisStreamName: "test-stream"}
	pub := NewPublisher(mock, cfg)

	events := make([]stream.Event, 5)
	for i := range events {
		events[i] = newTestEvent(fmt.Sprintf("e%d", i), "t1")
	}

	successes, failures, err := pub.PublishBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("expected no systemic error, got %v", err)
	}
	if successes != 2 {
		t.Errorf("expected 2 successes, got %d", successes)
	}
	if failures != 3 {
		t.Errorf("expected 3 failures, got %d", failures)
	}
}

func TestPublisher_PublishAfterClose(t *testing.T) {
	mock := &mockKinesisAPI{}
	cfg := stream.StreamConfig{KinesisStreamName: "test-stream"}
	pub := NewPublisher(mock, cfg)

	if err := pub.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	err := pub.Publish(context.Background(), newTestEvent("e1", "t1"))
	if err == nil {
		t.Fatal("expected error when publishing after close")
	}
}

func TestPublisher_PublishWithApiError(t *testing.T) {
	mock := &mockKinesisAPI{returnErr: fmt.Errorf("service unavailable")}
	cfg := stream.StreamConfig{KinesisStreamName: "test-stream"}
	pub := NewPublisher(mock, cfg)

	err := pub.Publish(context.Background(), newTestEvent("e1", "t1"))
	if err == nil {
		t.Fatal("expected error from API failure")
	}
}

func TestSubscriber_ConsumeAndCheckpoint(t *testing.T) {
	evt := newTestEvent("kin-evt-1", "t1")
	evtJSON, _ := json.Marshal(evt)

	readerMock := &mockKinesisReaderAPI{
		shards: []ShardInfo{{ShardID: "shard-0"}},
		records: map[string][]KinesisRecord{
			"shard-0": {
				{
					SequenceNumber: "seq-001",
					Data:           evtJSON,
					PartitionKey:   "t1",
					Timestamp:      time.Now(),
				},
			},
		},
	}

	cp := NewInMemoryCheckpointer()
	cfg := stream.StreamConfig{
		KinesisStreamName: "test-stream",
		BatchSize:         10,
		FlushInterval:     50 * time.Millisecond,
	}
	sub := NewSubscriber(readerMock, cp, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var received []stream.Event
	var mu sync.Mutex
	handler := func(_ context.Context, e stream.Event) error {
		mu.Lock()
		received = append(received, e)
		mu.Unlock()
		cancel()
		return nil
	}

	_ = sub.Subscribe(ctx, handler)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("expected 1 event, got %d", len(received))
	}
	if received[0].ID != "kin-evt-1" {
		t.Errorf("expected event ID 'kin-evt-1', got %q", received[0].ID)
	}

	// Check that the checkpoint was set.
	seq, err := cp.GetCheckpoint(context.Background(), "shard-0")
	if err != nil {
		t.Fatalf("get checkpoint failed: %v", err)
	}
	if seq != "seq-001" {
		t.Errorf("expected checkpoint 'seq-001', got %q", seq)
	}
}

// mockKinesisReaderAPI is a mock implementation of KinesisReaderAPI for testing.
type mockKinesisReaderAPI struct {
	shards  []ShardInfo
	records map[string][]KinesisRecord
	mu      sync.Mutex
	called  map[string]bool
}

func (m *mockKinesisReaderAPI) ListShards(_ context.Context, _ string) ([]ShardInfo, error) {
	return m.shards, nil
}

func (m *mockKinesisReaderAPI) GetShardIterator(_ context.Context, _, shardID, _ string) (string, error) {
	return "iterator-" + shardID, nil
}

func (m *mockKinesisReaderAPI) GetRecords(ctx context.Context, shardIterator string, _ int) (*GetRecordsOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.called == nil {
		m.called = make(map[string]bool)
	}

	// Return records only on first call per iterator.
	if m.called[shardIterator] {
		return &GetRecordsOutput{
			NextShardIterator: shardIterator,
		}, nil
	}
	m.called[shardIterator] = true

	// Extract shard ID from iterator name.
	shardID := shardIterator[len("iterator-"):]
	records := m.records[shardID]

	return &GetRecordsOutput{
		Records:           records,
		NextShardIterator: shardIterator,
	}, nil
}
