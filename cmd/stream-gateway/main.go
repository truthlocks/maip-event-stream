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

// Package main implements the MAIP Event Stream Gateway, a standalone HTTP
// server that publishes MAIP receipt events to a configurable stream backend.
// It exposes endpoints for single and batch publish, health checks, and
// Prometheus-compatible metrics. Configuration is loaded from environment
// variables.
//
// Usage:
//
//	MAIP_STREAM_BACKEND=redis REDIS_URL=redis://localhost:6379 ./stream-gateway
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/middleware"
	"github.com/truthlocks/maip-event-stream/pkg/receipt"
	"github.com/truthlocks/maip-event-stream/pkg/stream"

	// Register all backends so that the registry can resolve them by name.
	_ "github.com/truthlocks/maip-event-stream/pkg/eventbridge"
	_ "github.com/truthlocks/maip-event-stream/pkg/kinesis"
	_ "github.com/truthlocks/maip-event-stream/pkg/nats"
	_ "github.com/truthlocks/maip-event-stream/pkg/redis"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg := stream.LoadConfigFromEnv()
	if err := cfg.Validate(); err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	logger.Info("starting MAIP Event Stream Gateway",
		"backend", cfg.Backend,
		"batch_size", cfg.BatchSize,
		"max_retries", cfg.MaxRetries,
	)

	// A configured backend must provide a real client. Refuse startup rather
	// than accepting events that cannot be published.
	backends := stream.List()
	entry, err := stream.Get(cfg.Backend)
	if err != nil {
		logger.Error("backend not found", "error", err)
		os.Exit(1)
	}
	publisher, err := entry.NewPublisher(cfg)
	if err != nil || publisher == nil {
		logger.Error("backend client is unavailable; gateway will not accept events")
		os.Exit(1)
	}
	defer publisher.Close()

	// Set up middleware stack.
	collector := middleware.NewMetricsCollector()
	validator := receipt.DefaultValidator()
	transformer := receipt.NewTransformer("")

	// Build the HTTP mux.
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "healthy",
			"backend":  cfg.Backend,
			"backends": backends,
			"time":     time.Now().UTC(),
		})
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(collector.Snapshot())
	})

	mux.HandleFunc("/publish", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var rcpt receipt.MAIPReceipt
		if err := json.NewDecoder(r.Body).Decode(&rcpt); err != nil {
			http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
			return
		}

		if err := validator.Validate(rcpt); err != nil {
			http.Error(w, fmt.Sprintf("validation failed: %v", err), http.StatusUnprocessableEntity)
			return
		}

		evt, err := transformer.ReceiptToEvent(rcpt)
		if err != nil {
			http.Error(w, fmt.Sprintf("transform failed: %v", err), http.StatusInternalServerError)
			return
		}

		if err := publisher.Publish(r.Context(), evt); err != nil {
			http.Error(w, "backend did not confirm publication; inspect before retrying", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"event_id": evt.ID,
			"status":   "published",
			"backend":  cfg.Backend,
		})
	})

	mux.HandleFunc("/publish/batch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var receipts []receipt.MAIPReceipt
		if err := json.NewDecoder(r.Body).Decode(&receipts); err != nil {
			http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
			return
		}

		if len(receipts) == 0 {
			http.Error(w, "empty batch", http.StatusBadRequest)
			return
		}

		// Validate all receipts.
		validationErrors := validator.ValidateBatch(receipts)
		validReceipts := make([]receipt.MAIPReceipt, 0, len(receipts))
		invalidCount := 0
		for i, verr := range validationErrors {
			if verr != nil {
				invalidCount++
				logger.Warn("receipt validation failed",
					"receipt_id", receipts[i].ID,
					"error", verr,
				)
				continue
			}
			validReceipts = append(validReceipts, receipts[i])
		}

		events, transformErrors := transformer.BatchReceiptsToEvents(validReceipts)
		if len(transformErrors) > 0 {
			logger.Warn("some receipts failed transformation", "errors", len(transformErrors))
		}

		published, failed, publishErr := publisher.PublishBatch(r.Context(), events)
		status := http.StatusOK
		if invalidCount+len(transformErrors)+failed > 0 {
			status = http.StatusMultiStatus
		}
		if publishErr != nil {
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"total": len(receipts), "published": published, "failed": failed,
			"rejected": invalidCount + len(transformErrors), "backend": cfg.Backend,
		})
	})

	// Determine listen address.
	addr := os.Getenv("MAIP_GATEWAY_ADDR")
	if addr == "" {
		addr = ":8090"
	}

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("HTTP server listening", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown failed", "error", err)
		os.Exit(1)
	}

	logger.Info("gateway stopped gracefully")
}
