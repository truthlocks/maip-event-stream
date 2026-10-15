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

package middleware

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// MetricsCollector exposes counters and histograms for monitoring event
// publishing. It provides both a Prometheus-compatible interface (via the
// exported methods) and an internal atomic counter interface for lock-free
// reads.
type MetricsCollector struct {
	publishedTotal int64
	failedTotal    int64
	batchTotal     int64
	latencyNsSum   int64
	latencyCount   int64
}

// NewMetricsCollector creates a new MetricsCollector.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{}
}

// PublishedTotal returns the total number of successfully published events.
func (m *MetricsCollector) PublishedTotal() int64 {
	return atomic.LoadInt64(&m.publishedTotal)
}

// FailedTotal returns the total number of failed event publishes.
func (m *MetricsCollector) FailedTotal() int64 {
	return atomic.LoadInt64(&m.failedTotal)
}

// BatchTotal returns the total number of PublishBatch calls.
func (m *MetricsCollector) BatchTotal() int64 {
	return atomic.LoadInt64(&m.batchTotal)
}

// AverageLatencyMs returns the average publish latency in milliseconds.
// Returns 0 if no latency has been recorded.
func (m *MetricsCollector) AverageLatencyMs() float64 {
	count := atomic.LoadInt64(&m.latencyCount)
	if count == 0 {
		return 0
	}
	sum := atomic.LoadInt64(&m.latencyNsSum)
	return float64(sum) / float64(count) / float64(time.Millisecond)
}

// Snapshot returns a point-in-time copy of all metrics.
type MetricsSnapshot struct {
	// PublishedTotal is the total events published successfully.
	PublishedTotal int64 `json:"published_total"`
	// FailedTotal is the total events that failed to publish.
	FailedTotal int64 `json:"failed_total"`
	// BatchTotal is the total number of batch publish calls.
	BatchTotal int64 `json:"batch_total"`
	// AverageLatencyMs is the mean publish latency in milliseconds.
	AverageLatencyMs float64 `json:"average_latency_ms"`
}

// Snapshot returns a point-in-time copy of all metrics.
func (m *MetricsCollector) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		PublishedTotal:   m.PublishedTotal(),
		FailedTotal:      m.FailedTotal(),
		BatchTotal:       m.BatchTotal(),
		AverageLatencyMs: m.AverageLatencyMs(),
	}
}

func (m *MetricsCollector) recordPublish(count int, latency time.Duration) {
	atomic.AddInt64(&m.publishedTotal, int64(count))
	atomic.AddInt64(&m.latencyNsSum, int64(latency))
	atomic.AddInt64(&m.latencyCount, 1)
}

func (m *MetricsCollector) recordFailure(count int) {
	atomic.AddInt64(&m.failedTotal, int64(count))
}

func (m *MetricsCollector) recordBatch() {
	atomic.AddInt64(&m.batchTotal, 1)
}

// metricsPublisher wraps a Publisher with metrics collection.
type metricsPublisher struct {
	inner     stream.Publisher
	collector *MetricsCollector
}

// Metrics returns a Middleware that records publish metrics into the provided
// MetricsCollector. The collector's counters are updated atomically and are
// safe for concurrent reads.
func Metrics(collector *MetricsCollector) stream.Middleware {
	return func(pub stream.Publisher) stream.Publisher {
		return &metricsPublisher{
			inner:     pub,
			collector: collector,
		}
	}
}

// Publish implements stream.Publisher with metrics recording.
func (m *metricsPublisher) Publish(ctx context.Context, events ...stream.Event) error {
	start := time.Now()
	err := m.inner.Publish(ctx, events...)
	latency := time.Since(start)

	if err != nil {
		m.collector.recordFailure(len(events))
		return err
	}

	m.collector.recordPublish(len(events), latency)
	return nil
}

// PublishBatch implements stream.Publisher with metrics recording.
func (m *metricsPublisher) PublishBatch(ctx context.Context, events []stream.Event) (int, int, error) {
	start := time.Now()
	m.collector.recordBatch()

	successes, failures, err := m.inner.PublishBatch(ctx, events)
	latency := time.Since(start)

	if successes > 0 {
		m.collector.recordPublish(successes, latency)
	}
	if failures > 0 {
		m.collector.recordFailure(failures)
	}

	return successes, failures, err
}

// Close delegates to the inner publisher.
func (m *metricsPublisher) Close() error {
	return m.inner.Close()
}
