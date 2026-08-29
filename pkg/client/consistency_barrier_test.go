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

package client

import (
	"testing"
	"testing/synctest"

	. "github.com/onsi/gomega"
)

func TestKeyWriteBarrierSealWaitsForOverlappingBatches(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := NewWithT(t)
		b := &keyWriteBarrier{previous: closedChannel}

		releaseA := b.Begin()
		sealedDuringA := b.Seal()
		releaseB := b.Begin()
		sealedDuringB := b.Seal()

		synctest.Wait()
		g.Expect(sealedDuringA).NotTo(BeClosed(), "seal taken during A was released before any write finished")
		g.Expect(sealedDuringB).NotTo(BeClosed(), "seal taken during B was released before any write finished")

		releaseB()

		synctest.Wait()
		g.Expect(sealedDuringA).NotTo(BeClosed(), "seal taken during A was released while A was still in flight")
		g.Expect(sealedDuringB).NotTo(BeClosed(), "seal taken during B was released while A was still in flight")

		releaseA()

		synctest.Wait()
		g.Expect(sealedDuringA).To(BeClosed(), "seal taken during A was not released after all writes finished")
		g.Expect(sealedDuringB).To(BeClosed(), "seal taken during B was not released after all writes finished")
	})
}
