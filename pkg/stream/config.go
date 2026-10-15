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

package stream

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// BackendName enumerates supported stream backends.
const (
	BackendEventBridge = "eventbridge"
	BackendKinesis     = "kinesis"
	BackendRedis       = "redis"
	BackendNATS        = "nats"
)

// StreamConfig holds all configuration for the event stream system.
// It is populated from environment variables via LoadConfigFromEnv or
// constructed programmatically.
type StreamConfig struct {
	// Backend selects the active stream backend: "eventbridge", "kinesis", "redis", or "nats".
	Backend string

	// --- EventBridge ---

	// EventBusName is the EventBridge bus to publish to. Default: "maip-events".
	EventBusName string
	// DetailType is the EventBridge detail-type for receipt events. Default: "MAIPReceipt".
	DetailType string
	// AWSRegion is the AWS region for EventBridge and Kinesis. Default: "us-east-1".
	AWSRegion string
	// EventBridgeSource is the source field for EventBridge entries. Default: "maip.event-stream".
	EventBridgeSource string

	// --- Kinesis ---

	// KinesisStreamName is the Kinesis Data Stream name. Default: "maip-receipt-stream".
	KinesisStreamName string
	// PartitionKeyField specifies which Event field to use as the Kinesis partition key.
	// Supported values: "tenant_id" (default), "id".
	PartitionKeyField string

	// --- Redis ---

	// RedisURL is the Redis connection string. Default from REDIS_URL env.
	RedisURL string
	// RedisStreamKey is the Redis Streams key. Default: "maip:receipts".
	RedisStreamKey string
	// RedisMaxLen caps the stream length via MAXLEN. Zero selects the default 100000; a negative value disables trimming.
	RedisMaxLen int64
	// RedisConsumerGroup is the consumer group name for XREADGROUP. Default: "maip-consumers".
	RedisConsumerGroup string
	// RedisConsumerName identifies this consumer within the group.
	RedisConsumerName string

	// --- NATS ---

	// NatsURL is the NATS server URL. Default from NATS_URL env.
	NatsURL string
	// NatsSubject is the NATS JetStream subject pattern. Default: "maip.receipts.>".
	NatsSubject string
	// NatsStreamName is the JetStream stream name. Default: "MAIP_RECEIPTS".
	NatsStreamName string
	// NatsDurableName is the durable consumer name. Default: "maip-consumer".
	NatsDurableName string

	// --- Common ---

	// BatchSize is the maximum number of events per batch publish. Default: 25.
	BatchSize int
	// FlushInterval is the maximum time to buffer events before flushing. Default: 1s.
	FlushInterval time.Duration
	// MaxRetries is the number of retry attempts for transient failures. Default: 3.
	MaxRetries int
	// RetryBackoff is the base duration between retries (doubled each attempt). Default: 500ms.
	RetryBackoff time.Duration
}

// LoadConfigFromEnv populates a StreamConfig from environment variables,
// applying sensible defaults for any unset variable.
func LoadConfigFromEnv() StreamConfig {
	cfg := StreamConfig{
		Backend: envOrDefault("MAIP_STREAM_BACKEND", BackendEventBridge),

		// EventBridge
		EventBusName:      envOrDefault("MAIP_EVENTBRIDGE_BUS", "maip-events"),
		DetailType:        envOrDefault("MAIP_EVENTBRIDGE_DETAIL_TYPE", "MAIPReceipt"),
		AWSRegion:         envOrDefault("AWS_REGION", "us-east-1"),
		EventBridgeSource: envOrDefault("MAIP_EVENTBRIDGE_SOURCE", "maip.event-stream"),

		// Kinesis
		KinesisStreamName: envOrDefault("MAIP_KINESIS_STREAM", "maip-receipt-stream"),
		PartitionKeyField: envOrDefault("MAIP_KINESIS_PARTITION_KEY", "tenant_id"),

		// Redis
		RedisURL:           envOrDefault("REDIS_URL", "redis://localhost:6379"),
		RedisStreamKey:     envOrDefault("MAIP_REDIS_STREAM_KEY", "maip:receipts"),
		RedisMaxLen:        envOrDefaultInt64("MAIP_REDIS_MAXLEN", 100000),
		RedisConsumerGroup: envOrDefault("MAIP_REDIS_CONSUMER_GROUP", "maip-consumers"),
		RedisConsumerName:  envOrDefault("MAIP_REDIS_CONSUMER_NAME", "consumer-1"),

		// NATS
		NatsURL:         envOrDefault("NATS_URL", "nats://localhost:4222"),
		NatsSubject:     envOrDefault("MAIP_NATS_SUBJECT", "maip.receipts.>"),
		NatsStreamName:  envOrDefault("MAIP_NATS_STREAM_NAME", "MAIP_RECEIPTS"),
		NatsDurableName: envOrDefault("MAIP_NATS_DURABLE_NAME", "maip-consumer"),

		// Common
		BatchSize:     envOrDefaultInt("MAIP_STREAM_BATCH_SIZE", 25),
		FlushInterval: envOrDefaultDuration("MAIP_STREAM_FLUSH_INTERVAL", time.Second),
		MaxRetries:    envOrDefaultInt("MAIP_STREAM_MAX_RETRIES", 3),
		RetryBackoff:  envOrDefaultDuration("MAIP_STREAM_RETRY_BACKOFF", 500*time.Millisecond),
	}
	return cfg
}

// Validate checks that the configuration is internally consistent and returns
// an error describing any problems found.
func (c StreamConfig) Validate() error {
	switch c.Backend {
	case BackendEventBridge, BackendKinesis, BackendRedis, BackendNATS:
		// valid
	default:
		return fmt.Errorf("unsupported backend %q: must be one of eventbridge, kinesis, redis, nats", c.Backend)
	}

	if c.BatchSize < 1 {
		return fmt.Errorf("batch_size must be >= 1, got %d", c.BatchSize)
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be >= 0, got %d", c.MaxRetries)
	}
	if c.RetryBackoff <= 0 {
		return fmt.Errorf("retry_backoff must be > 0, got %v", c.RetryBackoff)
	}
	if c.FlushInterval <= 0 {
		return fmt.Errorf("flush_interval must be > 0, got %v", c.FlushInterval)
	}

	switch c.Backend {
	case BackendRedis:
		if c.RedisURL == "" {
			return fmt.Errorf("redis backend requires REDIS_URL")
		}
	case BackendNATS:
		if c.NatsURL == "" {
			return fmt.Errorf("nats backend requires NATS_URL")
		}
	case BackendKinesis:
		if c.KinesisStreamName == "" {
			return fmt.Errorf("kinesis backend requires a stream name")
		}
	case BackendEventBridge:
		if c.EventBusName == "" {
			return fmt.Errorf("eventbridge backend requires an event bus name")
		}
	}

	return nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrDefaultInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envOrDefaultInt64(key string, fallback int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return fallback
}

func envOrDefaultDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
