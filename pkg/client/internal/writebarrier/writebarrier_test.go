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
	"slices"
	"testing"
	"testing/synctest"

	. "github.com/onsi/gomega"
)

func TestKeyWriteBarrierSealWaitsForOverlappingBatches(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		g := NewWithT(t)
		barrier := NewWriteBarrier()

		releases := make([]func(), 0, 3)
		seals := make([]<-chan struct{}, 0, 3)

		for range 3 {
			release := barrier.Begin()
			seal := barrier.Seal()
			releases = append(releases, release)
			seals = append(seals, seal)
		}
		synctest.Wait()

		for _, seal := range seals {
			g.Expect(seal).NotTo(BeClosed(), "seal taken during writes was released before any write finished")
		}

		// release in reverse order so we can assert that all seals are closed only after the last write is released
		for i := range slices.Backward(releases) {
			releases[i]()
			synctest.Wait()

			for _, seal := range seals {
				if i != 0 {
					g.Expect(seal).NotTo(BeClosed(), "seal taken during writes was released before all write finished")
				} else {
					g.Expect(seal).To(BeClosed(), "seal wasn't released after all writes finished")
				}
			}
		}
	})
}
