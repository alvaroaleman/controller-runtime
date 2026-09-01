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
	"fmt"
	"sync"
	"testing"
	"testing/synctest"

	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
)

func TestKeyWriteBarrierSealWithoutInFlightWrites(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := NewWithT(t)
		barrier := NewWriteBarrier()

		g.Expect(barrier.Seal()).To(BeClosed(), "seal without any write was not released")

		release := barrier.Begin()
		release()
		synctest.Wait()

		g.Expect(barrier.Seal()).To(BeClosed(), "seal taken after all writes finished was not released")
	})
}

func TestKeyWriteBarrierSealDoesNotWaitForLaterWrites(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := NewWithT(t)
		barrier := NewWriteBarrier()

		sealedRelease := barrier.Begin()
		seal := barrier.Seal()
		_ = barrier.Begin()

		synctest.Wait()
		g.Expect(seal).NotTo(BeClosed())

		sealedRelease()
		synctest.Wait()
		g.Expect(seal).To(BeClosed(), "seal waited for a write that started after it was taken")
	})
}

func TestKeyWriteBarrierSealWaitsForAllWritesInItsBatch(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := NewWithT(t)
		barrier := NewWriteBarrier()

		first := barrier.Begin()
		second := barrier.Begin()
		seal := barrier.Seal()

		first()
		synctest.Wait()
		g.Expect(seal).NotTo(BeClosed(), "seal was released while a write of its batch was still in flight")

		second()
		synctest.Wait()
		g.Expect(seal).To(BeClosed())
	})
}

func TestKeyWriteBarrierSealWaitsForPreviousBatches(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := NewWithT(t)
		barrier := NewWriteBarrier()

		firstRelease := barrier.Begin()
		firstSeal := barrier.Seal()

		secondRelease := barrier.Begin()
		secondSeal := barrier.Seal()

		emptySeal := barrier.Seal()

		synctest.Wait()
		for _, seal := range []<-chan struct{}{firstSeal, secondSeal, emptySeal} {
			g.Expect(seal).NotTo(BeClosed())
		}

		secondRelease()
		synctest.Wait()
		g.Expect(firstSeal).NotTo(BeClosed())
		g.Expect(secondSeal).NotTo(BeClosed(), "seal was released before the previous batch finished")
		g.Expect(emptySeal).NotTo(BeClosed(), "seal without own batch was released before the previous batches finished")

		firstRelease()
		synctest.Wait()
		g.Expect(firstSeal).To(BeClosed())
		g.Expect(secondSeal).To(BeClosed())
		g.Expect(emptySeal).To(BeClosed())
	})
}

func TestWriteBarriersSealsAreKeyScoped(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := NewWithT(t)
		barriers := NewWriteBarriers(NewWriteBarrier)

		a := types.NamespacedName{Namespace: "ns", Name: "a"}
		b := types.NamespacedName{Namespace: "ns", Name: "b"}

		releaseA := barriers.Begin(a)
		g.Expect(barriers.Seal(b)).To(BeClosed(), "seal for a key without in-flight writes was not released")

		sealA := barriers.Seal(a)
		releaseB := barriers.Begin(b)
		sealB := barriers.Seal(b)

		synctest.Wait()
		g.Expect(sealA).NotTo(BeClosed())
		g.Expect(sealB).NotTo(BeClosed())

		releaseB()
		synctest.Wait()
		g.Expect(sealB).To(BeClosed())
		g.Expect(sealA).NotTo(BeClosed(), "seal was released by a write to a different key")

		releaseA()
		synctest.Wait()
		g.Expect(sealA).To(BeClosed())
	})
}

func TestWriteBarriersSealAll(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := NewWithT(t)
		barriers := NewWriteBarriers(NewWriteBarrier)

		g.Expect(barriers.SealAll()).To(BeEmpty(), "sealing without any in-flight write returned seals")

		a := types.NamespacedName{Namespace: "ns", Name: "a"}
		b := types.NamespacedName{Namespace: "ns", Name: "b"}

		releaseA := barriers.Begin(a)
		releaseB := barriers.Begin(b)

		seals := barriers.SealAll()
		g.Expect(seals).To(HaveLen(2), "sealing all did not return one seal per key with in-flight writes")

		synctest.Wait()
		g.Expect(numClosed(seals)).To(Equal(0))

		releaseA()
		synctest.Wait()
		g.Expect(numClosed(seals)).To(Equal(1), "sealing all did not release exactly the seal of the finished key")

		releaseB()
		synctest.Wait()
		g.Expect(numClosed(seals)).To(Equal(2))

		g.Expect(barriers.SealAll()).To(BeEmpty(), "sealing all returned seals for keys whose writes all finished")
	})
}

func TestWriteBarriersBarriersAreGarbageCollected(t *testing.T) {
	t.Parallel()

	g := NewWithT(t)
	barriers := NewWriteBarriers(NewWriteBarrier).(*writeBarriers)

	key := types.NamespacedName{Namespace: "ns", Name: "a"}

	first := barriers.Begin(key)
	second := barriers.Begin(key)
	g.Expect(barriers.data).To(HaveLen(1))

	first()
	g.Expect(barriers.data).To(HaveLen(1), "barrier was removed while a write was still in flight")

	second()
	g.Expect(barriers.data).To(BeEmpty(), "barrier was not removed after all its writes finished")
}

func TestWriteBarriersConcurrentWrites(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := NewWithT(t)
		barriers := NewWriteBarriers(NewWriteBarrier).(*writeBarriers)

		keys := make([]types.NamespacedName, 0, 3)
		for i := range 3 {
			keys = append(keys, types.NamespacedName{Namespace: "ns", Name: fmt.Sprintf("key-%d", i)})
		}

		var wg sync.WaitGroup
		for i := range 30 {
			wg.Go(func() {
				barriers.Begin(keys[i%len(keys)])()
			})
		}

		for _, seal := range barriers.SealAll() {
			<-seal
		}
		for _, key := range keys {
			<-barriers.Seal(key)
		}

		wg.Wait()
		synctest.Wait()
		g.Expect(barriers.data).To(BeEmpty(), "barriers were not removed after all writes finished")
	})
}

func numClosed(seals []<-chan struct{}) int {
	var closed int
	for _, seal := range seals {
		select {
		case <-seal:
			closed++
		default:
		}
	}

	return closed
}
