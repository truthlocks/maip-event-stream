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

// Package receipt provides transformations between MAIP receipt JSON payloads
// and the canonical stream Event format, plus integrity validation before
// publishing.
package receipt

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/truthlocks/maip-event-stream/pkg/stream"
)

// SupportedAlgorithms lists all 10 supported signing algorithms for MAIP receipts.
// ECDSA: ES256 (P-256), ES384 (P-384), ES512 (P-521)
// RSA PKCS#1 v1.5: RS256, RS384, RS512
// RSA-PSS: PS256, PS384, PS512
// EdDSA: Ed25519
var SupportedAlgorithms = []string{
	"ES256", "ES384", "ES512",
	"RS256", "RS384", "RS512",
	"PS256", "PS384", "PS512",
	"Ed25519",
}

// IsSupportedAlgorithm checks if the given algorithm string is one of the 10
// supported signing algorithms.
func IsSupportedAlgorithm(alg string) bool {
	for _, a := range SupportedAlgorithms {
		if a == alg {
			return true
		}
	}
	return false
}

// MAIPReceipt represents the full MAIP receipt JSON structure as stored in the
// transparency log or returned by the API. This is the external format; the
// Transformer converts it to/from the stream Event format.
type MAIPReceipt struct {
	// ID is the unique receipt identifier.
	ID string `json:"id"`
	// Type classifies the receipt (e.g. "data-flow", "delegation", "attestation").
	Type string `json:"type"`
	// TenantID is the owning organization.
	TenantID string `json:"tenant_id"`
	// AgentID identifies the machine agent involved.
	AgentID string `json:"agent_id"`
	// Algorithm is the signing algorithm used for this receipt's attestation.
	// Must be one of: ES256, ES384, ES512, RS256, RS384, RS512, PS256, PS384, PS512, Ed25519.
	Algorithm string `json:"algorithm,omitempty"`
	// ChainHash links this receipt to the prior receipt in the chain.
	ChainHash string `json:"chain_hash"`
	// PreviousHash is the hash of the previous receipt.
	PreviousHash string `json:"previous_hash"`
	// Payload is the receipt body content.
	Payload json.RawMessage `json:"payload"`
	// Metadata carries supplementary key-value pairs.
	Metadata map[string]string `json:"metadata,omitempty"`
	// CreatedAt records when the receipt was created.
	CreatedAt time.Time `json:"created_at"`
	// Source identifies the creating service.
	Source string `json:"source,omitempty"`
}

// Transformer converts between MAIPReceipt and stream.Event / stream.ReceiptEvent.
type Transformer struct {
	// DefaultSource is used when the receipt's Source field is empty.
	DefaultSource string
}

// NewTransformer creates a Transformer with the given default source identifier.
func NewTransformer(defaultSource string) *Transformer {
	if defaultSource == "" {
		defaultSource = "maip/event-stream"
	}
	return &Transformer{DefaultSource: defaultSource}
}

// ReceiptToEvent converts a MAIPReceipt to a stream.Event. The receipt body
// is serialized as the event payload, and receipt-specific fields are stored
// in metadata.
func (t *Transformer) ReceiptToEvent(r MAIPReceipt) (stream.Event, error) {
	payload, err := json.Marshal(r)
	if err != nil {
		return stream.Event{}, fmt.Errorf("receipt transformer: marshal receipt %s: %w", r.ID, err)
	}

	source := r.Source
	if source == "" {
		source = t.DefaultSource
	}

	eventType := "receipt." + normalizeReceiptType(r.Type)

	metadata := make(map[string]string)
	for k, v := range r.Metadata {
		metadata[k] = v
	}
	metadata["receipt_type"] = r.Type
	metadata["chain_hash"] = r.ChainHash
	metadata["agent_id"] = r.AgentID
	if r.Algorithm != "" {
		metadata["algorithm"] = r.Algorithm
	}
	if r.PreviousHash != "" {
		metadata["previous_hash"] = r.PreviousHash
	}

	return stream.Event{
		ID:        r.ID,
		Type:      eventType,
		Source:    source,
		TenantID:  r.TenantID,
		Payload:   payload,
		Metadata:  metadata,
		Timestamp: r.CreatedAt,
	}, nil
}

// ReceiptToReceiptEvent converts a MAIPReceipt to a stream.ReceiptEvent,
// which includes typed receipt fields for convenience.
func (t *Transformer) ReceiptToReceiptEvent(r MAIPReceipt) (stream.ReceiptEvent, error) {
	evt, err := t.ReceiptToEvent(r)
	if err != nil {
		return stream.ReceiptEvent{}, err
	}

	return stream.ReceiptEvent{
		Event:       evt,
		ReceiptID:   r.ID,
		ReceiptType: r.Type,
		ChainHash:   r.ChainHash,
		AgentID:     r.AgentID,
	}, nil
}

// EventToReceipt converts a stream.Event back to a MAIPReceipt by
// deserializing the payload. This is the inverse of ReceiptToEvent and
// enables round-trip fidelity.
func (t *Transformer) EventToReceipt(evt stream.Event) (MAIPReceipt, error) {
	var r MAIPReceipt
	if err := json.Unmarshal(evt.Payload, &r); err != nil {
		return MAIPReceipt{}, fmt.Errorf("receipt transformer: unmarshal event %s payload: %w", evt.ID, err)
	}

	// Overlay fields from the event envelope if the receipt's fields are empty.
	if r.ID == "" {
		r.ID = evt.ID
	}
	if r.TenantID == "" {
		r.TenantID = evt.TenantID
	}
	if r.Source == "" {
		r.Source = evt.Source
	}
	if r.CreatedAt.IsZero() {
		r.CreatedAt = evt.Timestamp
	}

	return r, nil
}

// BatchReceiptsToEvents converts a slice of MAIPReceipts to stream.Events.
// It returns the successfully converted events and any errors encountered.
func (t *Transformer) BatchReceiptsToEvents(receipts []MAIPReceipt) ([]stream.Event, []error) {
	events := make([]stream.Event, 0, len(receipts))
	var errs []error

	for _, r := range receipts {
		evt, err := t.ReceiptToEvent(r)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		events = append(events, evt)
	}

	return events, errs
}

// normalizeReceiptType converts a receipt type string to a dot-separated event
// type suffix. For example, "data-flow" becomes "data-flow", "delegation"
// stays "delegation".
func normalizeReceiptType(t string) string {
	if t == "" {
		return "unknown"
	}
	return t
}
