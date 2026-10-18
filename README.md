# Truthlocks MAIP event streams

Go building blocks for moving and describing machine-action evidence in your event pipeline.

## Setup

Install the Go library in your own module:

```sh
go get github.com/truthlocks/maip-event-stream@v0.1.1
```

Version 0.1.0 is an initial library release. Redis has a concrete client and an
isolated real-broker recovery test. Other backend adapters accept injected clients;
the standalone HTTP gateway is not ready for production multi-tenant use.

To work on the source:

```sh
cd packages/maip-event-stream
go test ./...
```

## Local example

Place the example in a separate command directory within a module that resolves this
package through your workspace or a verified module release:

```go
package main

import (
    "encoding/json"
    "fmt"
    "time"
    "github.com/truthlocks/maip-event-stream/pkg/stream"
)

func main() {
    event := stream.Event{
        ID: "example-event", Type: "receipt.created", Source: "example",
        TenantID: "example-workspace", Payload: json.RawMessage(`{"receipt_id":"example"}`),
        Timestamp: time.Now().UTC(),
    }
    encoded, err := json.Marshal(event)
    if err != nil { panic(err) }
    fmt.Println(string(encoded))
}
```

## Integration contract

Publisher and Subscriber interfaces support backend adapters for Redis Streams, NATS, Kinesis and EventBridge. The adapters accept backend-specific client interfaces. Redis includes a concrete go-redis client; the other backends still require a real client implementation and credentials. An interface implementation or fixture test does not establish a live broker connection. The example only encodes an event locally.

## Delivery and trust

Handle publisher errors and partial batch failures explicitly. Respect context
cancellation and close client resources during shutdown. Choose retry, acknowledgment
and deduplication behavior for your broker; this library does not promise universal
exactly-once delivery. Tenant fields route metadata and do not replace broker or API
authorization.

Local hashes and event metadata do not constitute a server signature, trusted receipt
verification or transparency-log inclusion proof. Retain the authoritative signed
record separately when your workflow requires those guarantees.

## Troubleshooting

For module errors, verify the import path and workspace replacement. For broker errors,
check connectivity, credentials, topic/stream existence and consumer permissions.
Unit tests cover adapter behavior with fixtures; run a scoped broker integration test
before enabling production delivery. Never log broker credentials or full sensitive
message payloads while diagnosing failures.

## Delivery middleware

```go
// base must be a real stream.Publisher using an authenticated backend client.
wrapped := stream.ApplyMiddleware(base,
    middleware.Dedupe(middleware.DedupeConfig{ExpectedItems: 100000}),
    middleware.Retry(middleware.RetryConfig{MaxRetries: 0}),
)
err := wrapped.Publish(ctx, event)
// Persist and inspect failures; never acknowledge source data solely on enqueue.
```

Import `github.com/truthlocks/maip-event-stream/pkg/middleware` for these wrappers.
The first middleware is outermost. Deduplication keys include tenant, source and ID,
and suppress only identical events whose publication succeeded. Reusing an ID with
changed content returns an error. The cache is bounded and in memory: eviction or
restart can allow duplicates, but a probabilistic match cannot drop a new event.
`FalsePositiveRate` is a deprecated compatibility field and is ignored.

Retries default to zero. If you explicitly enable retries, delivery is at least
once: a timeout may happen after backend acceptance. Batch retry processes one
event at a time, never guesses failed identities from aggregate counts, and stops
with an error when attempts are exhausted. This is slower than a native batch;
correctness takes priority over silently losing or replaying the wrong records.

## Standalone gateway limitation

The included backend factories require concrete provider client adapters that are
not yet supplied by this module. The standalone gateway now refuses startup when
it cannot construct a real publisher. Earlier code returned accepted responses
without sending events; those responses were not delivery evidence. Use the library
with explicit clients while gateway adapters and authenticated tenant routing are
completed. Do not expose an unauthenticated gateway as an enterprise audit service.

The receipt validator checks structure and optional local hash links. It does not
verify signatures, trusted keys, revocation, or transparency inclusion. Use the
Truthlocks verification API or an appropriate trusted-key verifier for that purpose.

## Acknowledgements and checkpoints

Redis, NATS and EventBridge retain pending acknowledgement handles when the backend
returns an error, so the same acknowledgement can be retried. A successful request
removes only the handle it confirmed, preserving any newer redelivery handle.

Kinesis stops a shard on malformed records, handler errors or checkpoint-write
failure; it never advances a later checkpoint over that failure. Resuming a stored
checkpoint requires a client implementing `CheckpointReader.GetShardIteratorAfter`
with the exact sequence number. The in-memory checkpointer remains development-only.
A failing shard cancels the other shard workers and returns the failure promptly.
Production adapters still need durable checkpoint storage and stream/shard lifecycle
handling appropriate to their provider.

### NATS subject routing compatibility

Tenant identifiers and event types use `nats.SubjectToken(value)` when building
`maip.receipts.{tenant}.{type}`. UUIDs and ASCII letters, digits, hyphens and
underscores remain unchanged; other UTF-8 bytes are percent encoded. For example,
`receipt.created` becomes `receipt%2Ecreated`, while `org.a` and `org-a` remain
separate tenants. Update exact-subject subscriptions from the former lossy
hyphen replacement before upgrading. Wildcard subscriptions remain compatible.
Missing event IDs, tenants or types and wildcard-containing base subjects are
rejected before sending. Success requires a nonempty server acknowledgement.
The injected client must impose its own in-flight timeout; cancellation is
checked before each send and cannot interrupt the legacy client interface.

## Redis subscriber failure handling

Consumer-group creation errors stop subscription unless Redis returns BUSYGROUP
(the group already exists). Read failures retry after 100 ms, doubling up to
five seconds; a successful read resets the delay. Context cancellation and Close
interrupt that wait. One subscription loop per Subscriber instance is allowed.
Provider wrappers should translate normal empty blocking reads to an empty result
with no error. On startup, the subscriber walks pending entries for its configured
consumer name before reading new messages. It repeats that scan every five seconds
between reads. Failed handlers are not acknowledged, and a cursor advances past
failed entries so later pending messages can still be processed. Handlers must be
idempotent: processing can repeat until the application acknowledges success.

Keep a stable, unique consumer name for each worker across restarts. This recovery
handles that consumer's pending entries by default. To recover entries from retired
consumers, opt in with `RedisClaimMinIdle: 10 * time.Minute` or
`MAIP_REDIS_CLAIM_MIN_IDLE=10m`. Choose a threshold **longer than the maximum valid
handler duration**: Redis measures delivery idle time, not whether a worker is alive.
A slow active handler can otherwise race with its replacement. Keep handlers
idempotent; this is at-least-once delivery, not exactly-once processing.

Recovery requires Redis 6.2+ and permission for
[XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/). Zero disables it;
negative or positive sub-millisecond values are rejected. One bounded claim batch
is scanned per pending-recovery cycle, with a cursor carried between cycles.
Claimed entries use the normal decode/handler/explicit-ACK path. Failed handlers
remain pending. Claim errors stop subscription visibly; the caller should retry
with backoff and alert on persistent failure. Custom adapters must implement
`RedisPendingClaimer` when opting in. This does not recover trimmed payloads.
Client wrappers must omit BLOCK for a negative `XReadGroupArgs.Block`, and implement
explicit pending-ID reads as well as `>` reads. Pending payloads that were trimmed
from the stream cannot be recovered. Use an appropriate retention configuration
and test your real broker wrapper before relying on durable delivery.


## Connect to a real Redis server

Use `redis.NewClient` with a Redis URL from your secret store (`rediss://` enables
TLS). `Ping` verifies connectivity; constructing the client does not. This is a
library connection, not an authenticated multi-tenant gateway deployment.

```go
package main

import (
    "context"
    "log"
    "os"
    "time"

    redisstream "github.com/truthlocks/maip-event-stream/pkg/redis"
    "github.com/truthlocks/maip-event-stream/pkg/stream"
)

func main() {
    client, err := redisstream.NewClient(os.Getenv("REDIS_URL"))
    if err != nil { log.Fatal("Invalid Redis configuration") }
    defer client.Close()
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    if err := client.Ping(ctx); err != nil { log.Fatal("Redis connection failed") }
    subscriber := redisstream.NewSubscriber(client, stream.StreamConfig{
        RedisStreamKey: "qa:receipts",
        RedisConsumerGroup: "qa-workers",
        RedisConsumerName: "worker-a", // stable across this worker's restarts
    })
    defer subscriber.Close()
    err = subscriber.Subscribe(ctx, func(ctx context.Context, event stream.Event) error {
        // Commit your idempotent processing before acknowledging the event.
        log.Printf("Received event %s", event.ID)
        return subscriber.Acknowledge(ctx, event.ID)
    })
    if err != nil && ctx.Err() == nil { log.Fatal("Subscription failed") }
}
```

The sample logs and acknowledges events; replace that handler with your durable
application processing. Publishers default to trimming at approximately 100,000
entries when `RedisMaxLen` is zero; set it negative to disable trimming and manage
retention explicitly. Trimming can remove payloads that have not been acknowledged.

Run `go test -race ./...` with `redis-server` on PATH to include the isolated real
broker restart/acknowledgement test. The test uses a temporary Unix socket, no
network listener and no production data. Without the binary that test is skipped;
fixture tests alone do not establish real broker acceptance.
