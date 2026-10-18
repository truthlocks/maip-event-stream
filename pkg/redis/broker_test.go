package redis

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// An isolated Unix-socket broker: no production URL, network listener or data.
func isolatedRedis(t *testing.T) (*Client, context.Context) {
	t.Helper()
	binary := os.Getenv("TRUTHLOCKS_REDIS_SERVER")
	if binary == "" {
		var err error
		binary, err = exec.LookPath("redis-server")
		if err != nil {
			t.Skip("redis-server is required for real broker acceptance")
		}
	}
	dir, err := os.MkdirTemp("", "tl-redis-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "redis.sock")
	cmd := exec.Command(binary, "--port", "0", "--unixsocket", socket, "--unixsocketperm", "700", "--save", "", "--appendonly", "no")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	c, err := NewClient("unix://" + socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	for c.Ping(ctx) != nil {
		if ctx.Err() != nil {
			t.Fatal("isolated Redis did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return c, ctx
}

func TestRealRedisRestartRecovery(t *testing.T) {
	c, ctx := isolatedRedis(t)
	cfg := stream.StreamConfig{RedisStreamKey: "qa:recovery", RedisConsumerGroup: "qa", RedisConsumerName: "worker-a", BatchSize: 1}
	p := NewPublisher(c, cfg)
	if err := p.Publish(ctx, newTestEvent("one", "qa"), newTestEvent("two", "qa"), newTestEvent("three", "qa")); err != nil {
		t.Fatal(err)
	}
	first := NewSubscriber(c, cfg)
	firstCtx, stopFirst := context.WithCancel(ctx)
	seen := 0
	err := first.Subscribe(firstCtx, func(context.Context, stream.Event) error {
		seen++
		if seen == 2 {
			stopFirst()
		}
		return errors.New("simulated failed worker")
	})
	if !errors.Is(err, context.Canceled) || seen != 2 {
		t.Fatalf("failed worker: %v count=%d", err, seen)
	}
	_ = first.Close()
	restarted := NewSubscriber(c, cfg)
	restartCtx, stopRestart := context.WithCancel(ctx)
	var recovered []string
	err = restarted.Subscribe(restartCtx, func(ctx context.Context, e stream.Event) error {
		recovered = append(recovered, e.ID)
		if e.ID == "one" {
			return errors.New("poison message remains pending")
		}
		if err := restarted.Acknowledge(ctx, e.ID); err != nil {
			t.Fatal(err)
		}
		if e.ID == "three" {
			stopRestart()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) || len(recovered) != 3 || recovered[0] != "one" || recovered[1] != "two" || recovered[2] != "three" {
		t.Fatalf("pending/new ordering: %v %v", recovered, err)
	}
	_ = restarted.Close()
	pending, err := c.client.XPending(ctx, cfg.RedisStreamKey, cfg.RedisConsumerGroup).Result()
	if err != nil || pending.Count != 1 {
		t.Fatalf("failed delivery was lost: %v %v", pending, err)
	}
	last := NewSubscriber(c, cfg)
	lastCtx, stopLast := context.WithCancel(ctx)
	err = last.Subscribe(lastCtx, func(ctx context.Context, e stream.Event) error {
		if e.ID != "one" {
			t.Fatalf("unexpected replay: %s", e.ID)
		}
		if err := last.Acknowledge(ctx, e.ID); err != nil {
			t.Fatal(err)
		}
		stopLast()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	pending, err = c.client.XPending(ctx, cfg.RedisStreamKey, cfg.RedisConsumerGroup).Result()
	if err != nil || pending.Count != 0 {
		t.Fatalf("acknowledged entries retained: %v %v", pending, err)
	}
	rows, err := c.XReadGroup(ctx, &XReadGroupArgs{Group: cfg.RedisConsumerGroup, Consumer: cfg.RedisConsumerName, Streams: []string{cfg.RedisStreamKey, ">"}, Count: 1, Block: 10})
	if err != nil || len(rows) != 0 {
		t.Fatalf("normal empty read must not fail: %v %v", rows, err)
	}
}

func TestRealRedisRetiredConsumerRecovery(t *testing.T) {
	c, ctx := isolatedRedis(t)
	cfg := stream.StreamConfig{RedisStreamKey: "qa:retired", RedisConsumerGroup: "qa", RedisConsumerName: "retired", BatchSize: 1}
	if err := NewPublisher(c, cfg).Publish(ctx, newTestEvent("recover-me", "qa")); err != nil {
		t.Fatal(err)
	}
	old := NewSubscriber(c, cfg)
	oldCtx, stopOld := context.WithCancel(ctx)
	err := old.Subscribe(oldCtx, func(context.Context, stream.Event) error {
		stopOld()
		return errors.New("crashed before acknowledgement")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertOwner := func(want string) {
		t.Helper()
		pending, err := c.client.XPending(ctx, cfg.RedisStreamKey, cfg.RedisConsumerGroup).Result()
		if err != nil || pending.Count != 1 || pending.Consumers[want] != 1 {
			t.Fatalf("pending owner %s: %+v %v", want, pending, err)
		}
	}
	cfg.RedisConsumerName = "replacement"
	for _, idle := range []time.Duration{0, time.Hour} {
		cfg.RedisClaimMinIdle = idle
		s := NewSubscriber(c, cfg)
		s.blockMs = 10
		short, stop := context.WithTimeout(ctx, 50*time.Millisecond)
		err := s.Subscribe(short, func(context.Context, stream.Event) error { t.Fatal("live/disabled claim stole an entry"); return nil })
		stop()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		assertOwner("retired")
	}
	cfg.RedisClaimMinIdle = time.Millisecond
	replacement := NewSubscriber(c, cfg)
	failedCtx, stopFailed := context.WithCancel(ctx)
	err = replacement.Subscribe(failedCtx, func(context.Context, stream.Event) error { stopFailed(); return errors.New("replacement also failed") })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertOwner("replacement")
	restarted := NewSubscriber(c, cfg)
	done, stopDone := context.WithCancel(ctx)
	calls := 0
	err = restarted.Subscribe(done, func(ctx context.Context, e stream.Event) error {
		calls++
		if e.ID != "recover-me" {
			t.Fatalf("wrong payload: %s", e.ID)
		}
		if err := restarted.Acknowledge(ctx, e.ID); err != nil {
			t.Fatal(err)
		}
		stopDone()
		return nil
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("recovery: %v calls=%d", err, calls)
	}
	pending, err := c.client.XPending(ctx, cfg.RedisStreamKey, cfg.RedisConsumerGroup).Result()
	if err != nil || pending.Count != 0 {
		t.Fatalf("ACK not durable: %+v %v", pending, err)
	}
}
