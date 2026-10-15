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
func TestRealRedisRestartRecovery(t *testing.T) {
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
	defer cancel()
	for c.Ping(ctx) != nil {
		if ctx.Err() != nil {
			t.Fatal("isolated Redis did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cfg := stream.StreamConfig{RedisStreamKey: "qa:recovery", RedisConsumerGroup: "qa", RedisConsumerName: "worker-a", BatchSize: 1}
	p := NewPublisher(c, cfg)
	if err := p.Publish(ctx, newTestEvent("one", "qa"), newTestEvent("two", "qa"), newTestEvent("three", "qa")); err != nil {
		t.Fatal(err)
	}
	first := NewSubscriber(c, cfg)
	firstCtx, stopFirst := context.WithCancel(ctx)
	seen := 0
	err = first.Subscribe(firstCtx, func(context.Context, stream.Event) error {
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
