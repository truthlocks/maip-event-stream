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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// ValidationError represents one or more validation failures on a receipt.
type ValidationError struct {
	// ReceiptID is the ID of the receipt that failed validation.
	ReceiptID string
	// Reasons lists every individual validation failure.
	Reasons []string
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("receipt %s validation failed: %s", e.ReceiptID, strings.Join(e.Reasons, "; "))
}

// Validator checks envelope structure and, when supplied, a local chain hash.
// It does not verify signatures, trusted keys, revocation or transparency inclusion.
type Validator struct {
	// RequireChainHash controls whether the chain_hash field must be non-empty.
	RequireChainHash bool
	// RequireAgentID controls whether the agent_id field must be non-empty.
	RequireAgentID bool
	// RequireAlgorithm controls whether the algorithm field must be non-empty.
	RequireAlgorithm bool
}

// NewValidator creates a receipt validator with the given options.
func NewValidator(requireChainHash, requireAgentID bool) *Validator {
	return &Validator{
		RequireChainHash: requireChainHash,
		RequireAgentID:   requireAgentID,
		RequireAlgorithm: false,
	}
}

// DefaultValidator requires a local chain hash and agent ID. Passing this
// structural check is not evidence of cryptographic verification.
func DefaultValidator() *Validator {
	return &Validator{
		RequireChainHash: true,
		RequireAgentID:   true,
		RequireAlgorithm: false,
	}
}

// StrictValidator returns a Validator that also requires a valid algorithm field.
func StrictValidator() *Validator {
	return &Validator{
		RequireChainHash: true,
		RequireAgentID:   true,
		RequireAlgorithm: true,
	}
}

// Validate checks a MAIPReceipt for structural completeness and chain hash
// integrity. It returns nil if the receipt is valid.
func (v *Validator) Validate(r MAIPReceipt) error {
	var reasons []string

	if r.ID == "" {
		reasons = append(reasons, "id is required")
	}
	if r.Type == "" {
		reasons = append(reasons, "type is required")
	}
	if r.TenantID == "" {
		reasons = append(reasons, "tenant_id is required")
	}
	if v.RequireAgentID && r.AgentID == "" {
		reasons = append(reasons, "agent_id is required")
	}
	if v.RequireChainHash && r.ChainHash == "" {
		reasons = append(reasons, "chain_hash is required")
	}
	if v.RequireAlgorithm && r.Algorithm == "" {
		reasons = append(reasons, "algorithm is required")
	}
	if r.Algorithm != "" && !IsSupportedAlgorithm(r.Algorithm) {
		reasons = append(reasons, fmt.Sprintf(
			"unsupported algorithm %q: must be one of ES256, ES384, ES512, RS256, RS384, RS512, PS256, PS384, PS512, Ed25519",
			r.Algorithm,
		))
	}
	if len(r.Payload) == 0 {
		reasons = append(reasons, "payload must not be empty")
	} else if !json.Valid(r.Payload) {
		reasons = append(reasons, "payload is not valid JSON")
	}
	if r.CreatedAt.IsZero() {
		reasons = append(reasons, "created_at is required")
	}

	// Verify chain hash integrity if both chain_hash and previous_hash are present.
	if r.ChainHash != "" && r.PreviousHash != "" {
		expectedHash := ComputeChainHash(r.PreviousHash, r.Payload)
		if r.ChainHash != expectedHash {
			reasons = append(reasons, fmt.Sprintf(
				"chain_hash mismatch: expected %s, got %s", expectedHash, r.ChainHash,
			))
		}
	}

	if len(reasons) > 0 {
		return &ValidationError{
			ReceiptID: r.ID,
			Reasons:   reasons,
		}
	}

	return nil
}

// ValidateBatch validates a slice of receipts, returning a slice of errors
// corresponding 1:1 to the input. Nil entries indicate valid receipts.
func (v *Validator) ValidateBatch(receipts []MAIPReceipt) []error {
	errs := make([]error, len(receipts))
	for i, r := range receipts {
		errs[i] = v.Validate(r)
	}
	return errs
}

// ComputeChainHash computes the expected chain hash given the previous hash
// and the current payload. The chain hash is SHA-256(previous_hash || payload).
func ComputeChainHash(previousHash string, payload json.RawMessage) string {
	h := sha256.New()
	h.Write([]byte(previousHash))
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

// HashPayload returns the SHA-256 hex digest of the given raw payload bytes.
func HashPayload(payload json.RawMessage) string {
	h := sha256.Sum256(payload)
	return hex.EncodeToString(h[:])
}
