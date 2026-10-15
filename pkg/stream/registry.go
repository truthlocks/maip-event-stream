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

package stream

import (
	"fmt"
	"sort"
	"sync"
)

// PublisherFactory creates a Publisher from the given StreamConfig.
type PublisherFactory func(cfg StreamConfig) (Publisher, error)

// SubscriberFactory creates a Subscriber from the given StreamConfig.
type SubscriberFactory func(cfg StreamConfig) (Subscriber, error)

// BackendEntry holds the factories for a single named backend.
type BackendEntry struct {
	// Name is the backend identifier used in StreamConfig.Backend.
	Name string
	// NewPublisher constructs a Publisher for this backend.
	NewPublisher PublisherFactory
	// NewSubscriber constructs a Subscriber for this backend.
	NewSubscriber SubscriberFactory
}

// Registry is a thread-safe registry of stream backend factories. Backends
// self-register during init() so that the gateway binary can resolve them
// by name at runtime without import-time coupling.
type Registry struct {
	mu       sync.RWMutex
	backends map[string]BackendEntry
}

// defaultRegistry is the package-level registry used by the top-level
// Register/Get/List functions.
var defaultRegistry = &Registry{
	backends: make(map[string]BackendEntry),
}

// Register adds a backend to the default registry. It panics if a backend
// with the same name is already registered, preventing silent overwrites.
func Register(entry BackendEntry) {
	defaultRegistry.Register(entry)
}

// Get retrieves a backend entry by name from the default registry.
func Get(name string) (BackendEntry, error) {
	return defaultRegistry.Get(name)
}

// List returns the sorted names of all registered backends.
func List() []string {
	return defaultRegistry.List()
}

// Register adds a backend to the registry. It panics if a backend with the
// same name is already registered.
func (r *Registry) Register(entry BackendEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.backends[entry.Name]; exists {
		panic(fmt.Sprintf("stream: backend %q already registered", entry.Name))
	}
	r.backends[entry.Name] = entry
}

// Get retrieves a backend entry by name. It returns an error if the backend
// is not registered.
func (r *Registry) Get(name string) (BackendEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.backends[name]
	if !ok {
		available := r.listLocked()
		return BackendEntry{}, fmt.Errorf("stream: unknown backend %q (available: %v)", name, available)
	}
	return entry, nil
}

// List returns the sorted names of all registered backends.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.listLocked()
}

func (r *Registry) listLocked() []string {
	names := make([]string, 0, len(r.backends))
	for name := range r.backends {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
