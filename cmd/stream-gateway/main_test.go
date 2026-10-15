package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestGatewayRefusesUnwiredBackend(t *testing.T) {
	if os.Getenv("TRUTHLOCKS_GATEWAY_CHILD") == "1" {
		main()
		return
	}
	for _, backend := range []string{"redis", "nats", "eventbridge", "kinesis"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGatewayRefusesUnwiredBackend$")
			cmd.Env = append(os.Environ(), "TRUTHLOCKS_GATEWAY_CHILD=1", "MAIP_STREAM_BACKEND="+backend, "REDIS_URL=redis://localhost:6379", "NATS_URL=nats://localhost:4222")
			out, err := cmd.CombinedOutput()
			if err == nil || ctx.Err() != nil || !strings.Contains(string(out), "gateway will not accept events") || strings.Contains(string(out), "HTTP server listening") {
				t.Fatalf("gateway did not fail closed: %v %s", err, out)
			}
		})
	}
}
