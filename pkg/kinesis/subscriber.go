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
	"log/slog"
	"sync"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// KinesisReaderAPI abstracts the Kinesis shard-level read operations.
type KinesisReaderAPI interface {
	ListShards(ctx context.Context, streamName string) ([]ShardInfo, error)
	GetShardIterator(ctx context.Context, streamName, shardID, iteratorType string) (string, error)
	GetRecords(ctx context.Context, shardIterator string, limit int) (*GetRecordsOutput, error)
}

// CheckpointReader supplies the exact sequence number when resuming a shard.
// Clients without this capability cannot resume a non-empty checkpoint.
type CheckpointReader interface {
	GetShardIteratorAfter(ctx context.Context, streamName, shardID, sequenceNumber string) (string, error)
}

// ShardInfo describes a single Kinesis shard.
type ShardInfo struct {
	ShardID string
}

// GetRecordsOutput mirrors the Kinesis GetRecords response.
type GetRecordsOutput struct {
	Records            []KinesisRecord
	NextShardIterator  string
	MillisBehindLatest int64
}

// KinesisRecord mirrors a single Kinesis record.
type KinesisRecord struct {
	SequenceNumber string
	Data           []byte
	PartitionKey   string
	Timestamp      time.Time
}

// Checkpointer persists the last-processed sequence number per shard so that
// consumption can resume after restarts without reprocessing.
type Checkpointer interface {
	GetCheckpoint(ctx context.Context, shardID string) (string, error)
	SetCheckpoint(ctx context.Context, shardID, sequenceNumber string) error
}

// InMemoryCheckpointer is a non-durable checkpointer for development and
// testing. Production deployments should use DynamoDB or another persistent
// store.
type InMemoryCheckpointer struct {
	mu          sync.Mutex
	checkpoints map[string]string
}

// NewInMemoryCheckpointer returns a new in-memory checkpointer.
func NewInMemoryCheckpointer() *InMemoryCheckpointer {
	return &InMemoryCheckpointer{
		checkpoints: make(map[string]string),
	}
}

// GetCheckpoint returns the stored sequence number for the given shard, or
// an empty string if no checkpoint exists.
func (c *InMemoryCheckpointer) GetCheckpoint(_ context.Context, shardID string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.checkpoints[shardID], nil
}

// SetCheckpoint records the latest processed sequence number for a shard.
func (c *InMemoryCheckpointer) SetCheckpoint(_ context.Context, shardID, sequenceNumber string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkpoints[shardID] = sequenceNumber
	return nil
}

// Subscriber consumes events from a Kinesis Data Stream using shard iterators.
// It manages multiple shards concurrently and checkpoints progress.
type Subscriber struct {
	client       KinesisReaderAPI
	checkpointer Checkpointer
	streamName   string
	batchSize    int
	pollInterval time.Duration
	logger       *slog.Logger

	mu     sync.Mutex
	closed bool
	cancel context.CancelFunc
}

// NewSubscriber creates a Kinesis subscriber that consumes from all shards
// of the configured stream.
func NewSubscriber(client KinesisReaderAPI, checkpointer Checkpointer, cfg stream.StreamConfig) *Subscriber {
	streamName := cfg.KinesisStreamName
	if streamName == "" {
		streamName = "maip-receipt-stream"
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 || batchSize > 10000 {
		batchSize = 100
	}

	if checkpointer == nil {
		checkpointer = NewInMemoryCheckpointer()
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 500 * time.Millisecond
	}
	return &Subscriber{
		client:       client,
		checkpointer: checkpointer,
		streamName:   streamName,
		batchSize:    batchSize,
		pollInterval: cfg.FlushInterval,
		logger:       slog.Default().With("component", "kinesis-subscriber"),
	}
}

// Subscribe begins consuming events from all shards. It blocks until ctx is
// cancelled.
func (s *Subscriber) Subscribe(ctx context.Context, handler stream.EventHandler) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("kinesis: subscriber is closed")
	}
	ctx, s.cancel = context.WithCancel(ctx)
	cancel := s.cancel
	defer cancel()
	s.mu.Unlock()

	shards, err := s.client.ListShards(ctx, s.streamName)
	if err != nil {
		return fmt.Errorf("kinesis: list shards: %w", err)
	}

	s.logger.Info("starting shard consumers", "stream", s.streamName, "shard_count", len(shards))

	var wg sync.WaitGroup
	errCh := make(chan error, len(shards))

	for _, shard := range shards {
		wg.Add(1)
		go func(shardID string) {
			defer wg.Done()
			if err := s.consumeShard(ctx, shardID, handler); err != nil && ctx.Err() == nil {
				errCh <- err
				cancel()
			}
		}(shard.ShardID)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return ctx.Err()
}

// consumeShard processes records from a single shard until ctx is cancelled.
func (s *Subscriber) consumeShard(ctx context.Context, shardID string, handler stream.EventHandler) error {
	checkpoint, err := s.checkpointer.GetCheckpoint(ctx, shardID)
	if err != nil {
		return fmt.Errorf("kinesis: read checkpoint: %w", err)
	}
	iteratorType := "TRIM_HORIZON"
	var iterator string
	if checkpoint != "" {
		reader, ok := s.client.(CheckpointReader)
		if !ok {
			return fmt.Errorf("kinesis: client cannot resume an exact checkpoint")
		}
		iteratorType = "AFTER_SEQUENCE_NUMBER"
		iterator, err = reader.GetShardIteratorAfter(ctx, s.streamName, shardID, checkpoint)
	} else {
		iterator, err = s.client.GetShardIterator(ctx, s.streamName, shardID, iteratorType)
	}
	if err != nil {
		return fmt.Errorf("kinesis: get shard iterator for %s: %w", shardID, err)
	}

	s.logger.Info("consuming shard", "shard_id", shardID, "iterator_type", iteratorType)

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		if iterator == "" {
			s.logger.Info("shard exhausted", "shard_id", shardID)
			return nil
		}

		output, err := s.client.GetRecords(ctx, iterator, s.batchSize)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			s.logger.Error("GetRecords failed", "shard_id", shardID, "error", err)
			continue
		}

		for _, rec := range output.Records {
			var event stream.Event
			if err := json.Unmarshal(rec.Data, &event); err != nil {
				s.logger.Error("failed to unmarshal record",
					"shard_id", shardID,
					"sequence_number", rec.SequenceNumber,
					"error", err,
				)
				return fmt.Errorf("kinesis: malformed record at %s: %w", rec.SequenceNumber, err)
			}

			if err := handler(ctx, event); err != nil {
				s.logger.Warn("handler returned error",
					"shard_id", shardID,
					"event_id", event.ID,
					"error", err,
				)
				return fmt.Errorf("kinesis: handler failed at %s: %w", rec.SequenceNumber, err)
			}

			if err := s.checkpointer.SetCheckpoint(ctx, shardID, rec.SequenceNumber); err != nil {
				s.logger.Error("failed to checkpoint",
					"shard_id", shardID,
					"sequence_number", rec.SequenceNumber,
					"error", err,
				)
				return fmt.Errorf("kinesis: persist checkpoint: %w", err)
			}
		}

		iterator = output.NextShardIterator
	}
}

// Acknowledge is a no-op for Kinesis; progress is tracked via checkpointing.
func (s *Subscriber) Acknowledge(_ context.Context, _ ...string) error {
	return nil
}

// Close stops the subscriber.
func (s *Subscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("subscriber closed")
	return nil
}
