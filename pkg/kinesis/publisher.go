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

// Package kinesis implements the MAIP event stream interfaces for AWS Kinesis
// Data Streams, supporting batched PutRecords with tenant-based partition key
// routing and shard iterator-based consumption with checkpointing.
package kinesis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// kinesisMaxBatch is the maximum records per PutRecords API call.
const kinesisMaxBatch = 500

// KinesisAPI abstracts the subset of the AWS Kinesis client used by this
// package. Production code supplies the real SDK client; tests supply a mock.
type KinesisAPI interface {
	PutRecords(ctx context.Context, input *PutRecordsInput) (*PutRecordsOutput, error)
}

// PutRecordsInput mirrors the AWS SDK PutRecordsInput.
type PutRecordsInput struct {
	StreamName string
	Records    []PutRecordsRequestEntry
}

// PutRecordsOutput mirrors the AWS SDK PutRecordsOutput.
type PutRecordsOutput struct {
	FailedRecordCount int
	Records           []PutRecordsResultEntry
}

// PutRecordsRequestEntry mirrors a single entry in a PutRecords call.
type PutRecordsRequestEntry struct {
	Data         []byte
	PartitionKey string
}

// PutRecordsResultEntry mirrors a single result entry from PutRecords.
type PutRecordsResultEntry struct {
	SequenceNumber string
	ShardID        string
	ErrorCode      string
	ErrorMessage   string
}

// Publisher sends MAIP events to an AWS Kinesis Data Stream. It partitions
// records by tenant ID (or event ID) and batches them into groups of up to
// 500 per PutRecords call.
type Publisher struct {
	client            KinesisAPI
	streamName        string
	partitionKeyField string
	logger            *slog.Logger
	mu                sync.Mutex
	closed            bool
}

// NewPublisher creates a Kinesis publisher using the provided API client.
func NewPublisher(client KinesisAPI, cfg stream.StreamConfig) *Publisher {
	streamName := cfg.KinesisStreamName
	if streamName == "" {
		streamName = "maip-receipt-stream"
	}
	pkField := cfg.PartitionKeyField
	if pkField == "" {
		pkField = "tenant_id"
	}

	return &Publisher{
		client:            client,
		streamName:        streamName,
		partitionKeyField: pkField,
		logger:            slog.Default().With("component", "kinesis-publisher"),
	}
}

// NewPublisherFromConfig creates a Kinesis publisher from a StreamConfig,
// suitable for use with the backend registry.
func NewPublisherFromConfig(cfg stream.StreamConfig) (stream.Publisher, error) {
	if cfg.KinesisStreamName == "" {
		return nil, fmt.Errorf("kinesis: stream name is required")
	}
	return nil, fmt.Errorf("kinesis: use NewPublisher with an explicit KinesisAPI client")
}

// partitionKey extracts the partition key from an event based on the
// configured field.
func (p *Publisher) partitionKey(evt stream.Event) string {
	switch p.partitionKeyField {
	case "id":
		return evt.ID
	default:
		if evt.TenantID != "" {
			return evt.TenantID
		}
		return evt.ID
	}
}

// Publish sends one or more events to Kinesis, batching into groups of 500.
func (p *Publisher) Publish(ctx context.Context, events ...stream.Event) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("kinesis: publisher is closed")
	}
	p.mu.Unlock()

	for i := 0; i < len(events); i += kinesisMaxBatch {
		end := i + kinesisMaxBatch
		if end > len(events) {
			end = len(events)
		}
		chunk := events[i:end]

		records := make([]PutRecordsRequestEntry, 0, len(chunk))
		for _, evt := range chunk {
			data, err := json.Marshal(evt)
			if err != nil {
				return fmt.Errorf("kinesis: marshal event %s: %w", evt.ID, err)
			}
			records = append(records, PutRecordsRequestEntry{
				Data:         data,
				PartitionKey: p.partitionKey(evt),
			})
		}

		output, err := p.client.PutRecords(ctx, &PutRecordsInput{
			StreamName: p.streamName,
			Records:    records,
		})
		if err != nil {
			p.logger.Error("PutRecords failed", "batch_size", len(records), "error", err)
			return fmt.Errorf("kinesis: PutRecords: %w", err)
		}
		if output.FailedRecordCount > 0 {
			for _, rec := range output.Records {
				if rec.ErrorCode != "" {
					p.logger.Warn("record failed",
						"error_code", rec.ErrorCode,
						"error_message", rec.ErrorMessage,
					)
				}
			}
			return fmt.Errorf("kinesis: %d of %d records failed", output.FailedRecordCount, len(records))
		}
	}

	return nil
}

// PublishBatch sends a batch of events and returns granular counts.
func (p *Publisher) PublishBatch(ctx context.Context, events []stream.Event) (int, int, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return 0, len(events), fmt.Errorf("kinesis: publisher is closed")
	}
	p.mu.Unlock()

	totalSuccess := 0
	totalFailed := 0

	for i := 0; i < len(events); i += kinesisMaxBatch {
		end := i + kinesisMaxBatch
		if end > len(events) {
			end = len(events)
		}
		chunk := events[i:end]

		records := make([]PutRecordsRequestEntry, 0, len(chunk))
		for _, evt := range chunk {
			data, err := json.Marshal(evt)
			if err != nil {
				p.logger.Error("failed to marshal event", "event_id", evt.ID, "error", err)
				totalFailed++
				continue
			}
			records = append(records, PutRecordsRequestEntry{
				Data:         data,
				PartitionKey: p.partitionKey(evt),
			})
		}

		if len(records) == 0 {
			continue
		}

		output, err := p.client.PutRecords(ctx, &PutRecordsInput{
			StreamName: p.streamName,
			Records:    records,
		})
		if err != nil {
			p.logger.Error("PutRecords batch failed", "batch_size", len(records), "error", err)
			totalFailed += len(records)
			continue
		}

		totalFailed += output.FailedRecordCount
		totalSuccess += len(records) - output.FailedRecordCount
	}

	return totalSuccess, totalFailed, nil
}

// Close marks the publisher as closed.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.logger.Info("publisher closed")
	return nil
}

func init() {
	stream.Register(stream.BackendEntry{
		Name:         stream.BackendKinesis,
		NewPublisher: NewPublisherFromConfig,
	})
}
