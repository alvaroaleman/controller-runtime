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

package client_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	kscheme "k8s.io/client-go/kubernetes/scheme"

	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("ConsistentClient", func() {
	var (
		cl      client.Client
		ctx     context.Context
		cancel  context.CancelFunc
		counter atomic.Uint64
	)

	BeforeEach(func(specCtx SpecContext) {
		// NB: Don't derive from the BeforeEach's context, Ginkgo cancels it when the
		// node returns and it thus would not outlive it, stopping the cache's watches.
		ctx, cancel = context.WithCancel(context.WithoutCancel(specCtx))

		c, err := cache.New(cfg, cache.Options{Scheme: kscheme.Scheme})
		Expect(err).NotTo(HaveOccurred())

		// Set up informers for types used through the consistent client.
		_, err = c.GetInformer(ctx, &corev1.ConfigMap{})
		Expect(err).NotTo(HaveOccurred())
		_, err = c.GetInformer(ctx, &corev1.Namespace{})
		Expect(err).NotTo(HaveOccurred())

		go func() {
			defer GinkgoRecover()
			Expect(c.Start(ctx)).To(Succeed())
		}()
		Expect(c.WaitForCacheSync(ctx)).To(BeTrue())

		cl, err = client.New(cfg, client.Options{
			Scheme: kscheme.Scheme,
			Cache: &client.CacheOptions{
				Reader:                             c,
				ReadYourOwnWriteConsistencyEnabled: new(true),
			},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		cancel()
	})

	newConfigMap := func(ns string) *corev1.ConfigMap {
		n := counter.Add(1)
		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("consistency-test-%d", n),
				Namespace: ns,
			},
			Data: map[string]string{"key": "value"},
		}
	}

	type writeResult struct {
		name    string
		deleted bool
		data    map[string]string
	}

	DescribeTable("write then read",
		func(ctx context.Context, write func(ctx context.Context, cl client.Client, cm *corev1.ConfigMap) (writeResult, error)) {
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("consistency-wtr-%d", counter.Add(1))}}
			Expect(cl.Create(ctx, ns)).To(Succeed())
			DeferCleanup(func(ctx context.Context) {
				Expect(client.IgnoreNotFound(cl.Delete(ctx, ns))).To(Succeed())
			})

			cm := newConfigMap(ns.Name)
			Expect(cl.Create(ctx, cm)).To(Succeed())
			DeferCleanup(func(ctx context.Context) {
				Expect(client.IgnoreNotFound(cl.Delete(ctx, cm))).To(Succeed())
			})

			done := make(chan struct{})
			fmt.Printf("%s: write cm started\n", time.Now())
			var result *writeResult
			var resultLock sync.Mutex
			writeDone := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				res, err := write(ctx, cl, cm)
				Expect(err).NotTo(HaveOccurred())
				resultLock.Lock()
				defer resultLock.Unlock()
				result = &res
			}()

			go func() {
				defer GinkgoRecover()
				defer func() {
					done <- struct{}{}
				}()

				//if result.deleted {
				//	err := cl.Get(ctx, client.ObjectKeyFromObject(cm), &corev1.ConfigMap{})
				//	Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected NotFound after delete, got: %v", err)
				//} else {
				got := &corev1.ConfigMap{}
				fmt.Printf("%s: Get started\n", time.Now())
				Expect(cl.Get(ctx, client.ObjectKeyFromObject(cm), got)).To(Succeed())
				fmt.Printf("%s: Get executed\n", time.Now())

				resultLock.Lock()
				defer resultLock.Unlock()
				Expect(result).NotTo(BeNil(), "write result should not be nil")
				Expect(got.Name).To(Equal(result.name))
				Expect(got.Data).To(Equal(result.data))
				//}
			}()

			go func() {
				defer GinkgoRecover()
				defer func() {
					done <- struct{}{}
				}()

				list := &corev1.ConfigMapList{}
				fmt.Printf("%s: List started\n", time.Now())
				Expect(cl.List(ctx, list, client.InNamespace(ns.Name))).To(Succeed())
				fmt.Printf("%s: List executed\n", time.Now())

				//if result.deleted {
				//	Expect(list.Items).To(BeEmpty(), "list should be empty after delete")
				//} else {
				Expect(list.Items).To(HaveLen(1), "list should contain exactly one ConfigMap")
				resultLock.Lock()
				defer resultLock.Unlock()
				Expect(result).NotTo(BeNil(), "write result should not be nil")
				Expect(list.Items[0].Name).To(Equal(result.name))
				Expect(list.Items[0].Data).To(Equal(result.data))
				//}
			}()

			<-done
			<-done
			<-writeDone
		},

		Entry("create", func(ctx context.Context, cl client.Client, cm *corev1.ConfigMap) (writeResult, error) {
			return writeResult{
				name: cm.Name,
				data: cm.Data,
			}, nil // already created in the setup
		}),

		FEntry("update", func(ctx context.Context, cl client.Client, cm *corev1.ConfigMap) (writeResult, error) {
			got := &corev1.ConfigMap{}
			if err := cl.Get(ctx, client.ObjectKeyFromObject(cm), got); err != nil {
				return writeResult{}, err
			}
			got.Data["key"] = "updated"
			if err := cl.Update(ctx, got); err != nil {
				return writeResult{}, err
			}
			return writeResult{
				name: cm.Name,
				data: map[string]string{"key": "updated"},
			}, nil
		}),

		Entry("patch", func(ctx context.Context, cl client.Client, cm *corev1.ConfigMap) (writeResult, error) {
			got := &corev1.ConfigMap{}
			if err := cl.Get(ctx, client.ObjectKeyFromObject(cm), got); err != nil {
				return writeResult{}, err
			}
			patch := client.MergeFrom(got.DeepCopy())
			got.Data["patched"] = "yes"
			if err := cl.Patch(ctx, got, patch); err != nil {
				return writeResult{}, err
			}
			return writeResult{
				name: cm.Name,
				data: map[string]string{"key": "value", "patched": "yes"},
			}, nil
		}),

		Entry("apply", func(ctx context.Context, cl client.Client, cm *corev1.ConfigMap) (writeResult, error) {
			ac := corev1ac.ConfigMap(cm.Name, cm.Namespace).
				WithData(map[string]string{"key": "applied"})
			if err := cl.Apply(ctx, ac, client.FieldOwner("consistency-test"), client.ForceOwnership); err != nil {
				return writeResult{}, err
			}
			return writeResult{
				name: cm.Name,
				data: map[string]string{"key": "applied"},
			}, nil
		}),

		Entry("delete", func(ctx context.Context, cl client.Client, cm *corev1.ConfigMap) (writeResult, error) {
			if err := cl.Delete(ctx, cm); err != nil {
				return writeResult{}, err
			}
			return writeResult{
				name:    cm.Name,
				deleted: true,
			}, nil
		}),
	)

	Describe("Delete object with finalizer then Get", func() {
		It("should observe the updated object with deletion timestamp after delete", func() {
			cm := newConfigMap("default")
			cm.Finalizers = []string{"test.io/hold"}
			Expect(cl.Create(ctx, cm)).To(Succeed())
			DeferCleanup(func(ctx context.Context) {
				got := &corev1.ConfigMap{}
				if err := cl.Get(ctx, client.ObjectKeyFromObject(cm), got); err == nil {
					got.Finalizers = nil
					Expect(cl.Update(ctx, got)).To(Succeed())
				}
			})

			got := &corev1.ConfigMap{}
			Expect(cl.Get(ctx, client.ObjectKeyFromObject(cm), got)).To(Succeed())

			Expect(cl.Delete(ctx, cm)).To(Succeed())

			afterDelete := &corev1.ConfigMap{}
			Expect(cl.Get(ctx, client.ObjectKeyFromObject(cm), afterDelete)).To(Succeed())
			Expect(afterDelete.DeletionTimestamp).NotTo(BeNil(), "should have a deletion timestamp")
		})
	})
})
