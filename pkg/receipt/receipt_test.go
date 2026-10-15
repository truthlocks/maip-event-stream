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

package receipt

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

func makeReceipt() MAIPReceipt {
	payload := json.RawMessage(`{"action": "data-transfer", "bytes": 1024}`)
	previousHash := "0000000000000000000000000000000000000000000000000000000000000000"
	return MAIPReceipt{
		ID:           "rcpt-001",
		Type:         "data-flow",
		TenantID:     "tenant-abc",
		AgentID:      "agent-42",
		ChainHash:    ComputeChainHash(previousHash, payload),
		PreviousHash: previousHash,
		Payload:      payload,
		Metadata:     map[string]string{"version": "1"},
		CreatedAt:    time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC),
		Source:       "maip/test-service",
	}
}

func TestTransformer_RoundTrip(t *testing.T) {
	tr := NewTransformer("maip/default")
	original := makeReceipt()

	// Receipt -> Event
	evt, err := tr.ReceiptToEvent(original)
	if err != nil {
		t.Fatalf("ReceiptToEvent failed: %v", err)
	}
	if evt.ID != "rcpt-001" {
		t.Errorf("expected event ID 'rcpt-001', got %q", evt.ID)
	}
	if evt.Type != "receipt.data-flow" {
		t.Errorf("expected event type 'receipt.data-flow', got %q", evt.Type)
	}
	if evt.TenantID != "tenant-abc" {
		t.Errorf("expected tenant 'tenant-abc', got %q", evt.TenantID)
	}
	if evt.Source != "maip/test-service" {
		t.Errorf("expected source 'maip/test-service', got %q", evt.Source)
	}
	if evt.Metadata["chain_hash"] != original.ChainHash {
		t.Errorf("metadata chain_hash mismatch")
	}

	// Event -> Receipt (round-trip)
	restored, err := tr.EventToReceipt(evt)
	if err != nil {
		t.Fatalf("EventToReceipt failed: %v", err)
	}
	if restored.ID != original.ID {
		t.Errorf("round-trip ID mismatch: %q vs %q", restored.ID, original.ID)
	}
	if restored.Type != original.Type {
		t.Errorf("round-trip Type mismatch: %q vs %q", restored.Type, original.Type)
	}
	if restored.AgentID != original.AgentID {
		t.Errorf("round-trip AgentID mismatch: %q vs %q", restored.AgentID, original.AgentID)
	}
	if restored.ChainHash != original.ChainHash {
		t.Errorf("round-trip ChainHash mismatch")
	}
}

func TestTransformer_ReceiptToReceiptEvent(t *testing.T) {
	tr := NewTransformer("maip/default")
	original := makeReceipt()

	re, err := tr.ReceiptToReceiptEvent(original)
	if err != nil {
		t.Fatalf("ReceiptToReceiptEvent failed: %v", err)
	}
	if re.ReceiptID != "rcpt-001" {
		t.Errorf("expected receipt ID 'rcpt-001', got %q", re.ReceiptID)
	}
	if re.ReceiptType != "data-flow" {
		t.Errorf("expected receipt type 'data-flow', got %q", re.ReceiptType)
	}
	if re.AgentID != "agent-42" {
		t.Errorf("expected agent ID 'agent-42', got %q", re.AgentID)
	}
}

func TestTransformer_DefaultSource(t *testing.T) {
	tr := NewTransformer("maip/custom-source")
	r := makeReceipt()
	r.Source = "" // empty => should use default

	evt, err := tr.ReceiptToEvent(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if evt.Source != "maip/custom-source" {
		t.Errorf("expected default source 'maip/custom-source', got %q", evt.Source)
	}
}

func TestTransformer_BatchReceiptsToEvents(t *testing.T) {
	tr := NewTransformer("")
	receipts := []MAIPReceipt{
		makeReceipt(),
		makeReceipt(),
	}
	receipts[1].ID = "rcpt-002"

	events, errs := tr.BatchReceiptsToEvents(receipts)
	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[1].ID != "rcpt-002" {
		t.Errorf("expected second event ID 'rcpt-002', got %q", events[1].ID)
	}
}

func TestTransformer_EventToReceiptOverlayFields(t *testing.T) {
	tr := NewTransformer("")

	// Create an event with a payload that has empty ID/TenantID.
	minimalReceipt := MAIPReceipt{
		Type:    "delegation",
		Payload: json.RawMessage(`{"scope": "read"}`),
	}
	payloadBytes, _ := json.Marshal(minimalReceipt)

	evt := stream.Event{
		ID:        "overlay-id",
		TenantID:  "overlay-tenant",
		Source:    "overlay-source",
		Payload:   payloadBytes,
		Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	restored, err := tr.EventToReceipt(evt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if restored.ID != "overlay-id" {
		t.Errorf("expected overlayed ID 'overlay-id', got %q", restored.ID)
	}
	if restored.TenantID != "overlay-tenant" {
		t.Errorf("expected overlayed tenant 'overlay-tenant', got %q", restored.TenantID)
	}
	if restored.Source != "overlay-source" {
		t.Errorf("expected overlayed source 'overlay-source', got %q", restored.Source)
	}
}

func TestValidator_ValidReceipt(t *testing.T) {
	v := DefaultValidator()
	r := makeReceipt()

	if err := v.Validate(r); err != nil {
		t.Fatalf("expected valid receipt, got error: %v", err)
	}
}

func TestValidator_MissingRequiredFields(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*MAIPReceipt)
		reason string
	}{
		{
			name:   "missing id",
			modify: func(r *MAIPReceipt) { r.ID = "" },
			reason: "id is required",
		},
		{
			name:   "missing type",
			modify: func(r *MAIPReceipt) { r.Type = "" },
			reason: "type is required",
		},
		{
			name:   "missing tenant_id",
			modify: func(r *MAIPReceipt) { r.TenantID = "" },
			reason: "tenant_id is required",
		},
		{
			name:   "missing agent_id",
			modify: func(r *MAIPReceipt) { r.AgentID = "" },
			reason: "agent_id is required",
		},
		{
			name:   "missing chain_hash",
			modify: func(r *MAIPReceipt) { r.ChainHash = "" },
			reason: "chain_hash is required",
		},
		{
			name:   "empty payload",
			modify: func(r *MAIPReceipt) { r.Payload = nil },
			reason: "payload must not be empty",
		},
		{
			name:   "invalid JSON payload",
			modify: func(r *MAIPReceipt) { r.Payload = json.RawMessage(`{broken`) },
			reason: "payload is not valid JSON",
		},
		{
			name:   "zero created_at",
			modify: func(r *MAIPReceipt) { r.CreatedAt = time.Time{} },
			reason: "created_at is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := DefaultValidator()
			r := makeReceipt()
			tt.modify(&r)

			err := v.Validate(r)
			if err == nil {
				t.Fatal("expected validation error")
			}
			ve, ok := err.(*ValidationError)
			if !ok {
				t.Fatalf("expected *ValidationError, got %T", err)
			}
			found := false
			for _, reason := range ve.Reasons {
				if reason == tt.reason {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected reason %q in %v", tt.reason, ve.Reasons)
			}
		})
	}
}

func TestValidator_ChainHashIntegrity(t *testing.T) {
	v := DefaultValidator()
	r := makeReceipt()
	r.ChainHash = "deadbeef" // tampered

	err := v.Validate(r)
	if err == nil {
		t.Fatal("expected chain hash validation error")
	}
	ve := err.(*ValidationError)
	found := false
	for _, reason := range ve.Reasons {
		if len(reason) >= 20 && reason[:20] == "chain_hash mismatch:" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected chain_hash mismatch reason, got %v", ve.Reasons)
	}
}

func TestValidator_RelaxedMode(t *testing.T) {
	v := NewValidator(false, false) // no chain_hash or agent_id required
	r := makeReceipt()
	r.ChainHash = ""
	r.AgentID = ""

	if err := v.Validate(r); err != nil {
		t.Fatalf("expected valid receipt in relaxed mode, got: %v", err)
	}
}

func TestValidator_ValidateBatch(t *testing.T) {
	v := DefaultValidator()
	valid := makeReceipt()
	invalid := makeReceipt()
	invalid.ID = ""

	errs := v.ValidateBatch([]MAIPReceipt{valid, invalid})
	if errs[0] != nil {
		t.Errorf("expected first receipt to be valid, got: %v", errs[0])
	}
	if errs[1] == nil {
		t.Error("expected second receipt to be invalid")
	}
}

func TestComputeChainHash(t *testing.T) {
	payload := json.RawMessage(`{"action": "test"}`)
	prev := "abc123"

	h1 := ComputeChainHash(prev, payload)
	h2 := ComputeChainHash(prev, payload)
	if h1 != h2 {
		t.Error("ComputeChainHash is not deterministic")
	}

	h3 := ComputeChainHash("different", payload)
	if h1 == h3 {
		t.Error("different previous hashes should produce different chain hashes")
	}
}

func TestHashPayload(t *testing.T) {
	payload := json.RawMessage(`{"a": 1}`)
	h1 := HashPayload(payload)
	h2 := HashPayload(payload)
	if h1 != h2 {
		t.Error("HashPayload is not deterministic")
	}
	if len(h1) != 64 {
		t.Errorf("expected 64 char hex digest, got %d", len(h1))
	}
}
