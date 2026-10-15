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

// Package middleware provides cross-cutting Publisher wrappers for retries,
// deduplication, and Prometheus metrics collection.
package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// RetryConfig configures the retry middleware behavior.
type RetryConfig struct {
	// MaxRetries is the maximum number of retry attempts (0 = no retries).
	MaxRetries int
	// BaseBackoff is the initial backoff duration. Each retry doubles it.
	BaseBackoff time.Duration
	// MaxBackoff caps the backoff duration.
	MaxBackoff time.Duration
}

// DefaultRetryConfig returns sensible retry defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:  0,
		BaseBackoff: 500 * time.Millisecond,
		MaxBackoff:  30 * time.Second,
	}
}

// retryPublisher wraps a Publisher with exponential backoff retry logic.
type retryPublisher struct {
	inner  stream.Publisher
	config RetryConfig
	logger *slog.Logger
}

// Retry returns a Middleware that retries failed Publish and PublishBatch
// calls with exponential backoff. Context cancellation is respected between
// retries.
func Retry(cfg RetryConfig) stream.Middleware {
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	return func(pub stream.Publisher) stream.Publisher {
		return &retryPublisher{
			inner:  pub,
			config: cfg,
			logger: slog.Default().With("component", "retry-middleware"),
		}
	}
}

// Publish retries one event at a time. Explicit retries provide at-least-once
// delivery: a timeout can occur after acceptance, so consumers must be idempotent.
func (r *retryPublisher) Publish(ctx context.Context, events ...stream.Event) error {
	for _, e := range events {
		var last error
		for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
			if err := r.wait(ctx, attempt); err != nil {
				return err
			}
			last = r.inner.Publish(ctx, e)
			if last == nil {
				break
			}
		}
		if last != nil {
			return last
		}
	}
	return nil
}

// PublishBatch never infers failed identities from aggregate batch counts.
// Each call contains one event; confirmed earlier events are not replayed.
func (r *retryPublisher) PublishBatch(ctx context.Context, events []stream.Event) (int, int, error) {
	total := 0
	for i, e := range events {
		var last error
		for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
			if err := r.wait(ctx, attempt); err != nil {
				return total, len(events) - i, err
			}
			successes, failures, err := r.inner.PublishBatch(ctx, []stream.Event{e})
			if successes < 0 || successes > 1 || failures < 0 || failures > 1 || successes+failures > 1 {
				return total, len(events) - i, fmt.Errorf("invalid single-event publisher counts")
			}
			if err == nil && failures == 0 {
				total += successes
				last = nil
				break
			}
			if successes == 1 {
				return total + 1, len(events) - i - 1, fmt.Errorf("publisher reported success with an error; inspect before retrying: %v", err)
			}
			last = err
			if last == nil {
				last = fmt.Errorf("publisher reported an event failure")
			}
		}
		if last != nil {
			return total, len(events) - i, last
		}
	}
	return total, 0, nil
}
func (r *retryPublisher) wait(ctx context.Context, attempt int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if attempt == 0 {
		return nil
	}
	timer := time.NewTimer(r.calculateBackoff(attempt))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Close delegates to the inner publisher.
func (r *retryPublisher) Close() error {
	return r.inner.Close()
}

// calculateBackoff returns the backoff duration for the given attempt number
// using exponential backoff with a cap.
func (r *retryPublisher) calculateBackoff(attempt int) time.Duration {
	backoff := time.Duration(math.Min(float64(r.config.MaxBackoff), float64(r.config.BaseBackoff)*math.Pow(2, float64(attempt-1))))
	if backoff > r.config.MaxBackoff {
		backoff = r.config.MaxBackoff
	}
	return backoff
}
