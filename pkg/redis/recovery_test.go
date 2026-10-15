package redis

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

type recoveryClient struct {
	mockRedisStreamAPI
	positions []string
	t         *testing.T
}

func (m *recoveryClient) XReadGroup(ctx context.Context, a *XReadGroupArgs) ([]XStream, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	position := a.Streams[1]
	m.positions = append(m.positions, position)
	if a.Group != "durable-group" || a.Consumer != "durable-worker" {
		m.t.Fatal("consumer identity changed")
	}
	if position != ">" && a.Block >= 0 {
		m.t.Fatal("pending read must not block")
	}
	var id string
	switch position {
	case "0":
		id = "1-0"
	case "1-0":
		id = "2-0"
	case "2-0":
		return nil, nil
	case ">":
		id = "3-0"
	default:
		m.t.Fatalf("unexpected cursor %s", position)
	}
	b, _ := json.Marshal(newTestEvent(id, "tenant"))
	return []XStream{{Stream: a.Streams[0], Messages: []XMessage{{ID: id, Values: map[string]interface{}{"data": string(b)}}}}}, nil
}

func TestRestartRecoversPendingWithoutPoisonStarvation(t *testing.T) {
	m := &recoveryClient{t: t}
	s := NewSubscriber(m, stream.StreamConfig{RedisConsumerGroup: "durable-group", RedisConsumerName: "durable-worker"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := s.Subscribe(ctx, func(ctx context.Context, e stream.Event) error {
		if e.ID == "1-0" {
			return errors.New("poison message")
		}
		if err := s.Acknowledge(ctx, e.ID); err != nil {
			t.Fatal(err)
		}
		if e.ID == "3-0" {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected exit: %v", err)
	}
	if len(m.acked) != 2 || m.acked[0] != "2-0" || m.acked[1] != "3-0" {
		t.Fatalf("failed message acknowledged or later messages starved: %v", m.acked)
	}
	if len(m.positions) != 4 {
		t.Fatalf("pending scan did not finish before new reads: %v", m.positions)
	}
}
