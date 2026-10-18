package redis

import (
	"context"
	"fmt"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

// Client adapts go-redis to the stream interfaces. Use rediss:// for TLS.
// Constructing a client does not prove connectivity; Ping checks the real server.
type Client struct{ client *redisclient.Client }

func NewClient(url string) (*Client, error) {
	options, err := redisclient.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redis: invalid connection URL")
	}
	options.ContextTimeoutEnabled = true
	return &Client{client: redisclient.NewClient(options)}, nil
}

func (c *Client) Ping(ctx context.Context) error { return c.client.Ping(ctx).Err() }
func (c *Client) Close() error                   { return c.client.Close() }
func (c *Client) XGroupCreateMkStream(ctx context.Context, key, group, start string) error {
	return c.client.XGroupCreateMkStream(ctx, key, group, start).Err()
}
func (c *Client) XAck(ctx context.Context, key, group string, ids ...string) (int64, error) {
	return c.client.XAck(ctx, key, group, ids...).Result()
}
func (c *Client) XAdd(ctx context.Context, a *XAddArgs) (string, error) {
	return c.client.XAdd(ctx, &redisclient.XAddArgs{Stream: a.Stream, MaxLen: a.MaxLen, Approx: a.Approx, ID: a.ID, Values: a.Values}).Result()
}
func (c *Client) XReadGroup(ctx context.Context, a *XReadGroupArgs) ([]XStream, error) {
	rows, err := c.client.XReadGroup(ctx, &redisclient.XReadGroupArgs{Group: a.Group, Consumer: a.Consumer, Streams: a.Streams, Count: a.Count, Block: time.Duration(a.Block) * time.Millisecond, NoAck: a.NoAck}).Result()
	if err == redisclient.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]XStream, 0, len(rows))
	for _, row := range rows {
		item := XStream{Stream: row.Stream}
		for _, message := range row.Messages {
			item.Messages = append(item.Messages, XMessage{ID: message.ID, Values: message.Values})
		}
		result = append(result, item)
	}
	return result, nil
}

var _ RedisStreamAPI = (*Client)(nil)

// RedisPendingClaimer is optional for custom adapters; it requires Redis 6.2+.
// ClaimPending transfers ownership without acknowledging or invoking handlers.
type RedisPendingClaimer interface {
	ClaimPending(context.Context, string, string, string, time.Duration, string, int64) (string, error)
}

func (c *Client) ClaimPending(ctx context.Context, key, group, consumer string, idle time.Duration, start string, count int64) (string, error) {
	_, next, err := c.client.XAutoClaim(ctx, &redisclient.XAutoClaimArgs{
		Stream: key, Group: group, Consumer: consumer, MinIdle: idle, Start: start, Count: count,
	}).Result()
	return next, err
}

var _ RedisPendingClaimer = (*Client)(nil)
