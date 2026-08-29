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

package writebarrier

import (
	"sync"

	"k8s.io/apimachinery/pkg/types"
)

type WriteBarriers interface {
	Begin(key types.NamespacedName) func()
	Seal(key types.NamespacedName) <-chan struct{}
	SealAll() []<-chan struct{}
}

func NewWriteBarriers(newBarrier func() WriteBarrier) WriteBarriers {
	return &writeBarriers{
		data:       map[types.NamespacedName]*writeBarrierWithRefCounter{},
		newBarrier: newBarrier,
	}
}

type writeBarrierWithRefCounter struct {
	WriteBarrier
	inFlightWrites int
}

// writeBarriers holds one writeBarrier per key that has an in-flight write.
type writeBarriers struct {
	lock       sync.Mutex
	data       map[types.NamespacedName]*writeBarrierWithRefCounter
	newBarrier func() WriteBarrier
}

func (w *writeBarriers) Begin(key types.NamespacedName) func() {
	w.lock.Lock()
	defer w.lock.Unlock()

	barrier, exists := w.data[key]
	if !exists {
		barrier = &writeBarrierWithRefCounter{WriteBarrier: w.newBarrier()}
		w.data[key] = barrier
	}
	barrier.inFlightWrites++
	release := barrier.Begin()

	return func() {
		release()

		w.lock.Lock()
		defer w.lock.Unlock()
		barrier.inFlightWrites--
		if barrier.inFlightWrites == 0 {
			delete(w.data, key)
		}
	}
}

func (w *writeBarriers) Seal(key types.NamespacedName) <-chan struct{} {
	w.lock.Lock()
	defer w.lock.Unlock()

	barrier, exists := w.data[key]
	if !exists {
		return closedChannel
	}

	return barrier.Seal()
}

func (w *writeBarriers) SealAll() []<-chan struct{} {
	w.lock.Lock()
	defer w.lock.Unlock()

	result := make([]<-chan struct{}, 0, len(w.data))
	for _, barrier := range w.data {
		result = append(result, barrier.Seal())
	}

	return result
}

var closedChannel chan struct{}

func init() {
	closedChannel = make(chan struct{})
	close(closedChannel)
}

type writeBatch struct {
	barrier  *KeyWriteBarrier
	inFlight int
	done     chan struct{}
}

func (w *writeBatch) release() {
	w.barrier.mutex.Lock()
	defer w.barrier.mutex.Unlock()

	w.inFlight--
	if w.inFlight > 0 {
		return
	}

	close(w.done)
	if w.barrier.current == w {
		w.barrier.current = nil
	}
}

type WriteBarrier interface {
	Begin() (release func())
	Seal() <-chan struct{}
}

// KeyWriteBarrier allows to wait for a set of in-flight writes to finish.
type KeyWriteBarrier struct {
	// mutex must be held to access current
	mutex   sync.Mutex
	current *writeBatch
}

// Begin adds a write to the current batch, starting one if needed.
func (b *KeyWriteBarrier) Begin() func() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.current == nil {
		b.current = &writeBatch{barrier: b, done: make(chan struct{})}
	}
	b.current.inFlight++

	return b.current.release
}

// Seal seals the current write batch and returns a channel that closes
// once all writes in the batch are done.
func (b *KeyWriteBarrier) Seal() <-chan struct{} {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.current == nil {
		return closedChannel
	}

	done := b.current.done
	b.current = nil
	return done
}
