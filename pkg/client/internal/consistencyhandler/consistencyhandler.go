/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package consistencyhandler

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache/cacheapi"
)

func NewHandler(log logr.Logger) *ConsistencyHandler {
	return &ConsistencyHandler{
		rvBroadCaster:             &broadcaster{},
		pendingDeletesBroadcaster: &broadcaster{},
		pendingDeletesLock:        sync.RWMutex{},
		pendingDeletes:            make(map[types.NamespacedName]sets.Set[types.UID]),
		minimumRVsLock:            sync.Mutex{},
		minimumRVs:                make(map[types.NamespacedName]int64),
		log:                       log,
	}
}

type ConsistencyHandler struct {
	rvBroadCaster *broadcaster
	observedRV    atomic.Int64

	pendingDeletesBroadcaster *broadcaster
	pendingDeletesLock        sync.RWMutex
	// pendingDeletes holds pending deletes. Must only be accessed when holding pendingDeletesLock
	pendingDeletes map[types.NamespacedName]sets.Set[types.UID]

	minimumRVsLock sync.Mutex
	// minimumRVs stores the minimum RVs we must have seen before returning reads.
	minimumRVs map[types.NamespacedName]int64

	registerLock sync.Mutex
	registered   bool
	registration *cache.ResourceEventHandlerRegistration

	log logr.Logger
}

func (h *ConsistencyHandler) Registered() bool {
	h.registerLock.Lock()
	defer h.registerLock.Unlock()
	return h.registered
}

func (h *ConsistencyHandler) Register(ctx context.Context, informer cacheapi.Informer) error {
	h.registerLock.Lock()
	defer h.registerLock.Unlock()

	// Check again in case it got registered while we waited for the lock
	if h.registered {
		return nil
	}

	if h.registration != nil {
		return h.waitForHandlerSyncLocked(ctx, *h.registration)
	}

	registration, err := informer.AddEventHandler(h)
	if err != nil {
		return fmt.Errorf("failed to add consistency handler to informer: %w", err)
	}

	return h.waitForHandlerSyncLocked(ctx, registration)
}

func (h *ConsistencyHandler) waitForHandlerSyncLocked(ctx context.Context, registration cache.ResourceEventHandlerRegistration) error {
	select {
	case <-registration.HasSyncedChecker().Done():
		h.registered = true
		return nil
	case <-ctx.Done():
		return fmt.Errorf("failed waiting for consistency handler to sync: %w", ctx.Err())
	}
}

func (h *ConsistencyHandler) SetMinimumRV(key types.NamespacedName, rv int64) {
	h.minimumRVsLock.Lock()
	defer h.minimumRVsLock.Unlock()
	h.minimumRVs[key] = rv
}

func (h *ConsistencyHandler) getMinimumRVForKey(key types.NamespacedName) int64 {
	h.minimumRVsLock.Lock()
	defer h.minimumRVsLock.Unlock()
	return h.minimumRVs[key]
}

func (h *ConsistencyHandler) getMinimumRVForGVK() int64 {
	h.minimumRVsLock.Lock()
	defer h.minimumRVsLock.Unlock()
	var maxRV int64
	for _, rv := range h.minimumRVs {
		if rv > maxRV {
			maxRV = rv
		}
	}
	return maxRV
}

func (h *ConsistencyHandler) cleanupMinimumRVs(currentRV int64) {
	h.minimumRVsLock.Lock()
	defer h.minimumRVsLock.Unlock()
	for key, rv := range h.minimumRVs {
		if rv <= currentRV {
			delete(h.minimumRVs, key)
		}
	}
}

func (h *ConsistencyHandler) AddPendingDelete(key types.NamespacedName, uid types.UID) {
	h.pendingDeletesLock.Lock()
	defer h.pendingDeletesLock.Unlock()

	if h.pendingDeletes[key] == nil {
		h.pendingDeletes[key] = sets.New(uid)
		return
	}
	h.pendingDeletes[key].Insert(uid)
}

func (h *ConsistencyHandler) RemovePendingDelete(key types.NamespacedName, uid types.UID) {
	h.pendingDeletesLock.Lock()
	defer h.pendingDeletesLock.Unlock()

	if h.pendingDeletes[key] != nil {
		h.pendingDeletes[key].Delete(uid)
		if len(h.pendingDeletes[key]) == 0 {
			delete(h.pendingDeletes, key)
		}
		h.pendingDeletesBroadcaster.broadcast()
	}
}

func (h *ConsistencyHandler) WaitForList(ctx context.Context) error {
	if err := h.waitForRV(ctx, h.getMinimumRVForGVK()); err != nil {
		return err
	}

	return h.waitAllDeletes(ctx)
}

func (h *ConsistencyHandler) WaitForGet(ctx context.Context, key types.NamespacedName) error {
	if err := h.waitForRV(ctx, h.getMinimumRVForKey(key)); err != nil {
		return err
	}

	return h.waitDeletesForKey(ctx, key)
}

// waitDeletesForKey blocks until all pending deletes at the time of calling it were observed or context times out
func (h *ConsistencyHandler) waitDeletesForKey(ctx context.Context, key types.NamespacedName) error {
	h.pendingDeletesLock.RLock()
	pendingDeletes := maps.Clone(h.pendingDeletes[key])
	h.pendingDeletesLock.RUnlock()

	return h.waitDeletes(ctx, pendingDeletes)
}

// waitDeletesForGVK blocks until all pending deletes at the time of calling it were observed or context times out
func (h *ConsistencyHandler) waitAllDeletes(ctx context.Context) error {
	h.pendingDeletesLock.RLock()
	pendingDeletes := sets.Set[types.UID]{}
	for _, uids := range h.pendingDeletes {
		maps.Copy(pendingDeletes, uids)
	}
	h.pendingDeletesLock.RUnlock()

	return h.waitDeletes(ctx, pendingDeletes)
}

func (h *ConsistencyHandler) waitDeletes(ctx context.Context, uids sets.Set[types.UID]) error {
	if len(uids) == 0 {
		return nil
	}

	for {
		// must store the chan before checking the deletes to guarantee that even if the deletes
		// get  updated after our check and before the select, we still get an event.
		updatedChan := h.pendingDeletesBroadcaster.wait()
		h.pendingDeletesLock.RLock()
		done := h.allDeletedLocked(uids)
		h.pendingDeletesLock.RUnlock()
		if done {
			return nil
		}

		select {
		case <-updatedChan:
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *ConsistencyHandler) allDeletedLocked(uids sets.Set[types.UID]) bool {
	for wantDeleted := range uids {
		for _, notDeletedUIDs := range h.pendingDeletes {
			if notDeletedUIDs.Has(wantDeleted) {
				return false
			}
		}
	}

	return true
}

func (h *ConsistencyHandler) observeDeletion(obj cacheapi.Object) {
	key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
	h.pendingDeletesLock.Lock()
	defer h.pendingDeletesLock.Unlock()

	if h.pendingDeletes[key].Has(obj.GetUID()) {
		h.pendingDeletes[key].Delete(obj.GetUID())
		h.pendingDeletesBroadcaster.broadcast()
	}
	if len(h.pendingDeletes[key]) == 0 {
		delete(h.pendingDeletes, key)
	}
}

func (h *ConsistencyHandler) waitForRV(ctx context.Context, rv int64) error {
	for {
		// must store the chan before checking the RV to guarantee that even if the RV
		// gets updated after our check and before the select, we still get an event.
		updatedChan := h.rvBroadCaster.wait()
		if h.observedRV.Load() >= rv {
			return nil
		}
		select {
		case <-updatedChan:
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *ConsistencyHandler) observeResourceVersion(rv string) {
	parsed, err := strconv.ParseInt(rv, 10, 64)
	if err != nil {
		h.log.Error(err, "Failed to parse resource version as int64", "resourceVersion", rv)
		return
	}

	for {
		current := h.observedRV.Load()
		if parsed <= current {
			return
		}

		if h.observedRV.CompareAndSwap(current, parsed) {
			break
		}
	}

	h.rvBroadCaster.broadcast()

	go h.cleanupMinimumRVs(parsed)
}

func (h *ConsistencyHandler) OnAdd(raw any, _ bool) {
	obj, ok := raw.(cacheapi.Object)
	if !ok {
		h.log.Error(nil, "OnAdd received object that is not a cacheapi.Object", "object", raw)
		return
	}
	go func() { h.observeResourceVersion(obj.GetResourceVersion()) }()
}

func (h *ConsistencyHandler) OnUpdate(_, newObj any) {
	obj, ok := newObj.(cacheapi.Object)
	if !ok {
		h.log.Error(nil, "OnUpdate received object that is not a cacheapi.Object", "object", newObj)
		return
	}
	go func() { h.observeResourceVersion(obj.GetResourceVersion()) }()
}

func (h *ConsistencyHandler) OnDelete(raw any) {
	var obj cacheapi.Object
	switch t := raw.(type) {
	case cacheapi.Object:
		obj = t
	case cache.DeletedFinalStateUnknown:
		obj = t.Obj.(cacheapi.Object)
	default:
		h.log.Error(nil, "OnDelete received object that is not a cacheapi.Object or DeletedFinalStateUnknown", "object", raw)
		return
	}
	go func() { h.observeResourceVersion(obj.GetResourceVersion()) }()
	go func() { h.observeDeletion(obj) }()
}

// broadcaster wakes arbitrarily many waiters on broadcast().
type broadcaster struct {
	lock sync.Mutex
	ch   chan struct{}
}

func (b *broadcaster) broadcast() {
	b.lock.Lock()
	defer b.lock.Unlock()
	// Lazily create the channel in wait to avoid creating a channel per event.
	if b.ch != nil {
		close(b.ch)
		b.ch = nil
	}
}

func (b *broadcaster) wait() <-chan struct{} {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.ch == nil {
		b.ch = make(chan struct{})
	}

	return b.ch
}
