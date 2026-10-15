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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// DedupeConfig configures a bounded exact in-memory set of confirmed events.
type DedupeConfig struct {
	// ExpectedItems is the maximum retained successful event identities. Default 100000.
	ExpectedItems uint
	// FalsePositiveRate is deprecated and ignored. Events are never dropped on a probabilistic match.
	FalsePositiveRate float64
}

func DefaultDedupeConfig() DedupeConfig { return DedupeConfig{ExpectedItems: 100000} }

type eventIdentity struct{ tenant, source, id string }
type dedupePublisher struct {
	inner stream.Publisher
	gate  chan struct{}
	seen  map[eventIdentity][32]byte
	order []eventIdentity
	next  int
	limit uint
}

// Dedupe suppresses only confirmed, identical events within a bounded in-memory
// window. Failures are not cached. Eviction/restart can cause duplicates, never
// probabilistic loss. It does not replace durable broker deduplication.
func Dedupe(cfg DedupeConfig) stream.Middleware {
	if cfg.ExpectedItems == 0 {
		cfg.ExpectedItems = 100000
	}
	return func(pub stream.Publisher) stream.Publisher {
		return &dedupePublisher{inner: pub, gate: make(chan struct{}, 1), seen: make(map[eventIdentity][32]byte), limit: cfg.ExpectedItems}
	}
}
func (d *dedupePublisher) one(ctx context.Context, e stream.Event, batch bool) (int, error) {
	if e.ID == "" || e.TenantID == "" || e.Source == "" {
		return 0, fmt.Errorf("deduplication requires event ID, tenant and source")
	}
	encoded, err := json.Marshal(e)
	if err != nil {
		return 0, err
	}
	digest := sha256.Sum256(encoded)
	key := eventIdentity{e.TenantID, e.Source, e.ID}
	select {
	case d.gate <- struct{}{}:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	defer func() { <-d.gate }()
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	if previous, ok := d.seen[key]; ok {
		if previous != digest {
			return 0, fmt.Errorf("event ID reused with different content")
		}
		return 0, nil
	}
	count := 1
	if batch {
		successes, failures, e := d.inner.PublishBatch(ctx, []stream.Event{e})
		if e != nil {
			return 0, e
		}
		if failures != 0 || successes < 0 || successes > 1 {
			return 0, fmt.Errorf("single event was not confirmed by publisher")
		}
		count = successes
	} else if err = d.inner.Publish(ctx, e); err != nil {
		return 0, err
	}
	if uint(len(d.order)) < d.limit {
		d.order = append(d.order, key)
	} else {
		delete(d.seen, d.order[d.next])
		d.order[d.next] = key
		d.next = (d.next + 1) % len(d.order)
	}
	d.seen[key] = digest
	return count, nil
}
func (d *dedupePublisher) Publish(ctx context.Context, events ...stream.Event) error {
	for _, e := range events {
		if _, err := d.one(ctx, e, false); err != nil {
			return err
		}
	}
	return nil
}

// PublishBatch processes one event at a time so successful identities are known.
// On failure, failures includes the failed event and the unattempted suffix.
func (d *dedupePublisher) PublishBatch(ctx context.Context, events []stream.Event) (int, int, error) {
	successes := 0
	for i, e := range events {
		n, err := d.one(ctx, e, true)
		successes += n
		if err != nil {
			return successes, len(events) - i, err
		}
	}
	return successes, 0, nil
}
func (d *dedupePublisher) Close() error { return d.inner.Close() }
