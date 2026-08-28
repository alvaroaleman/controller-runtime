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
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/cache/cacheapi"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/client/internal/consistencyhandler"
)

type consistentClientUpstream interface {
	Client

	delete(ctx context.Context, obj Object, opts ...DeleteOption) (*unstructured.Unstructured, error)
}

type writeBarrier interface {
	Begin() (release func())
	Seal() <-chan struct{}
}

var _ Client = (*consistentClient)(nil)

func newConsistentClient(
	upstream consistentClientUpstream,
	informers cacheapi.Informers,
	newWriteBarrier func() writeBarrier,
	log logr.Logger,
) *consistentClient {
	return &consistentClient{
		upstream:  upstream,
		informers: informers,
		writeBarriersByGVK: newThreadSafeMap[schema.GroupVersionKind](func() *writeBarriers {
			return newWriteBarriers(newWriteBarrier)
		}),
		consistencyHandlers: newThreadSafeMap[schema.GroupVersionKind](func() *consistencyhandler.ConsistencyHandler {
			return consistencyhandler.NewHandler(log)
		}),
	}
}

type consistentClient struct {
	upstream  consistentClientUpstream
	informers cacheapi.Informers

	// writeBarriersByGVK maps gvk -> key -> writeBarrier
	writeBarriersByGVK *threadSafeMap[schema.GroupVersionKind, *writeBarriers]

	consistencyHandlers *threadSafeMap[schema.GroupVersionKind, *consistencyhandler.ConsistencyHandler]
}

func (c *consistentClient) getConsistencyHandler(ctx context.Context, gvk schema.GroupVersionKind, obj cacheapi.Object) (*consistencyhandler.ConsistencyHandler, error) {
	h := c.consistencyHandlers.getOrCreate(gvk)
	if h.Registered() {
		return h, nil
	}

	informer, err := c.informers.GetInformer(ctx, obj, cacheapi.BlockUntilSynced(true))
	if err != nil {
		return nil, fmt.Errorf("failed to get informer for GVK %s: %w", gvk, err)
	}

	if err := h.Register(ctx, informer); err != nil {
		return nil, fmt.Errorf("failed to register consistency handler on informer for GVK %s: %w", gvk, err)
	}
	return h, nil
}

func (c *consistentClient) Get(ctx context.Context, key ObjectKey, obj Object, opts ...GetOption) error {
	gvk, err := apiutil.GVKForObject(obj, c.upstream.Scheme())
	if err != nil {
		return fmt.Errorf("failed to get GVK for object %T: %w", obj, err)
	}

	select {
	case <-c.writeBarriersByGVK.getOrCreate(gvk).seal(key):
	case <-ctx.Done():
		return ctx.Err()
	}

	h, err := c.getConsistencyHandler(ctx, gvk, obj)
	if err != nil {
		return err
	}
	if err := h.WaitForGet(ctx, key); err != nil {
		return err
	}

	return c.upstream.Get(ctx, key, obj, opts...)
}

func (c *consistentClient) List(ctx context.Context, list ObjectList, opts ...ListOption) error {
	gvk, err := apiutil.GVKForObject(list, c.upstream.Scheme())
	if err != nil {
		return fmt.Errorf("failed to get GVK for list %T: %w", list, err)
	}
	gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")

	for _, s := range c.writeBarriersByGVK.getOrCreate(gvk).sealAll() {
		select {
		case <-s:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	listObj, err := c.upstream.Scheme().New(gvk)
	if err != nil {
		return fmt.Errorf("failed to create object for GVK %s: %w", gvk, err)
	}
	cacheObj, ok := listObj.(cacheapi.Object)
	if !ok {
		return fmt.Errorf("object of type %T for GVK %s does not implement cacheapi.Object", listObj, gvk)
	}

	h, err := c.getConsistencyHandler(ctx, gvk, cacheObj)
	if err != nil {
		return err
	}
	if err := h.WaitForList(ctx); err != nil {
		return err
	}

	return c.upstream.List(ctx, list, opts...)
}

func (c *consistentClient) Create(ctx context.Context, obj Object, opts ...CreateOption) error {
	return c.writeAndRecordRV(ctx, obj, func() error {
		return c.upstream.Create(ctx, obj, opts...)
	})
}

func (c *consistentClient) Update(ctx context.Context, obj Object, opts ...UpdateOption) error {
	return c.writeAndRecordRV(ctx, obj, func() error {
		return c.upstream.Update(ctx, obj, opts...)
	})
}

func (c *consistentClient) Patch(ctx context.Context, obj Object, patch Patch, opts ...PatchOption) error {
	return c.writeAndRecordRV(ctx, obj, func() error {
		return c.upstream.Patch(ctx, obj, patch, opts...)
	})
}

func (c *consistentClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...ApplyOption) error {
	return c.writeAndRecordRV(ctx, obj, func() error {
		return c.upstream.Apply(ctx, obj, opts...)
	})
}

func writeTargetFor(obj any, scheme *runtime.Scheme) (schema.GroupVersionKind, types.NamespacedName, Object, func() (string, error), error) {
	switch t := obj.(type) {
	case *unstructuredApplyConfiguration:
		return t.Unstructured.GroupVersionKind(),
			types.NamespacedName{Namespace: t.Unstructured.GetNamespace(), Name: t.Unstructured.GetName()},
			t.Unstructured,
			func() (string, error) { return t.Unstructured.GetResourceVersion(), nil },
			nil
	case applyConfiguration:
		gvk, err := gvkFromApplyConfiguration(t)
		if err != nil {
			return schema.GroupVersionKind{}, types.NamespacedName{}, nil, nil, fmt.Errorf("failed to get GVK for apply configuration %T: %w", obj, err)
		}
		cacheObj, err := scheme.New(gvk)
		if err != nil {
			return schema.GroupVersionKind{}, types.NamespacedName{}, nil, nil, fmt.Errorf("failed to create cache object for GVK %s: %w", gvk, err)
		}
		clientObj, ok := cacheObj.(Object)
		if !ok {
			return schema.GroupVersionKind{}, types.NamespacedName{}, nil, nil, fmt.Errorf("cache object of type %T for GVK %s does not implement client.Object", cacheObj, gvk)
		}
		clientObj.SetName(ptr.Deref(t.GetName(), ""))
		clientObj.SetNamespace(ptr.Deref(t.GetNamespace(), ""))
		return gvk,
			types.NamespacedName{Namespace: ptr.Deref(t.GetNamespace(), ""), Name: ptr.Deref(t.GetName(), "")},
			clientObj,
			func() (string, error) { return resourceVersionFromApplyConfiguration(t) },
			nil
	case Object:
		gvk, err := apiutil.GVKForObject(t, scheme)
		if err != nil {
			return schema.GroupVersionKind{}, types.NamespacedName{}, nil, nil, fmt.Errorf("failed to get GVK for object %T: %w", obj, err)
		}
		return gvk,
			types.NamespacedName{Namespace: t.GetNamespace(), Name: t.GetName()},
			t,
			func() (string, error) { return t.GetResourceVersion(), nil },
			nil
	default:
		return schema.GroupVersionKind{}, types.NamespacedName{}, nil, nil, fmt.Errorf("unsupported type %T, must be either %T, %T or %T", obj, Object(nil), &unstructuredApplyConfiguration{}, applyConfiguration(nil))
	}
}

func (c *consistentClient) writeAndRecordRV(ctx context.Context, obj any, write func() error) error {
	gvk, namespacedName, cacheObj, getResourceVersion, err := writeTargetFor(obj, c.upstream.Scheme())
	if err != nil {
		return err
	}

	// We don't technically need an informer since the RV is monotonically increasing, but we want to fail
	// ASAP if the cache can not be setup.
	if _, err := c.getConsistencyHandler(ctx, gvk, cacheObj); err != nil {
		return err
	}

	release := c.writeBarriersByGVK.getOrCreate(gvk).Begin(namespacedName)
	defer release()

	if err := write(); err != nil {
		return err
	}

	rvRaw, err := getResourceVersion()
	if err != nil {
		return fmt.Errorf("failed to get resource version from %T: %w", obj, err)
	}
	rv, err := strconv.ParseInt(rvRaw, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse resource version %s: %w", rvRaw, err)
	}

	h, err := c.getConsistencyHandler(ctx, gvk, cacheObj)
	if err != nil {
		return err
	}
	h.SetMinimumRV(ObjectKey{Namespace: cacheObj.GetNamespace(), Name: cacheObj.GetName()}, rv)

	return nil
}

func resourceVersionFromApplyConfiguration(obj applyConfiguration) (string, error) {
	v := reflect.ValueOf(obj)
	for v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return "", fmt.Errorf("expected struct, got %s", v.Kind())
	}
	rv := v.FieldByName("ResourceVersion")
	if !rv.IsValid() {
		return "", fmt.Errorf("type %T has no ResourceVersion field", obj)
	}
	if rv.Kind() != reflect.Pointer || rv.Type().Elem().Kind() != reflect.String {
		return "", fmt.Errorf("ResourceVersion field in %T is not *string", obj)
	}
	if rv.IsNil() {
		return "", fmt.Errorf("ResourceVersion field in %T is nil", obj)
	}
	return rv.Elem().String(), nil
}

func (c *consistentClient) Delete(ctx context.Context, obj Object, opts ...DeleteOption) error {
	gvk, err := apiutil.GVKForObject(obj, c.upstream.Scheme())
	if err != nil {
		return fmt.Errorf("failed to get GVK for object %v: %w", obj, err)
	}

	namespacedName := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}

	uid, err := c.uidForDelete(ctx, gvk, namespacedName, obj, opts...)
	if err != nil {
		return err
	}

	release := c.writeBarriersByGVK.getOrCreate(gvk).Begin(namespacedName)
	defer release()

	h, err := c.getConsistencyHandler(ctx, gvk, obj)
	if err != nil {
		return err
	}

	// Register the delete before we execute it, otherwise it may be in the cache
	// before we register it, causing a deadlock.
	h.AddPendingDelete(namespacedName, uid)

	response, err := c.upstream.delete(ctx, obj, opts...)
	if err != nil {
		h.RemovePendingDelete(namespacedName, uid)
		return err
	}

	if rvRaw := response.GetResourceVersion(); rvRaw != "" {
		h.RemovePendingDelete(namespacedName, uid)
		rv, err := strconv.ParseInt(rvRaw, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse resource version %s: %w", rvRaw, err)
		}
		h.SetMinimumRV(namespacedName, rv)
	}

	return nil
}

func (c *consistentClient) uidForDelete(ctx context.Context, gvk schema.GroupVersionKind, key ObjectKey, obj Object, opts ...DeleteOption) (types.UID, error) {
	deleteOptions := (&DeleteOptions{}).ApplyOptions(opts)
	if p := deleteOptions.Preconditions; p != nil && ptr.Deref(p.UID, "") != "" {
		return *p.UID, nil
	}

	if uid := obj.GetUID(); uid != "" {
		return uid, nil
	}

	existing, ok := obj.DeepCopyObject().(Object)
	if !ok {
		return "", fmt.Errorf("deepcopy of %T does not implement client.Object", obj)
	}
	if err := c.upstream.Get(ctx, key, existing); err != nil {
		return "", fmt.Errorf("failed to get %s %s to determine its uid: %w", gvk.Kind, key, err)
	}

	return existing.GetUID(), nil
}

func (c *consistentClient) DeleteAllOf(ctx context.Context, obj Object, opts ...DeleteAllOfOption) error {
	return errors.New("DeleteAllOf is not supported by consistentClient, please use List and Delete instead")
}

func (c *consistentClient) Status() SubResourceWriter {
	return c.SubResource("status")
}

func (c *consistentClient) Scheme() *runtime.Scheme {
	return c.upstream.Scheme()
}

func (c *consistentClient) RESTMapper() meta.RESTMapper {
	return c.upstream.RESTMapper()
}

func (c *consistentClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return c.upstream.GroupVersionKindFor(obj)
}

func (c *consistentClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	return c.upstream.IsObjectNamespaced(obj)
}

func (c *consistentClient) SubResource(subResource string) SubResourceClient {
	return &consistentSubResourceClient{
		writeAndRecordRV: c.writeAndRecordRV,
		upstream:         c.upstream.SubResource(subResource),
	}
}

type consistentSubResourceClient struct {
	writeAndRecordRV func(context.Context, any, func() error) error
	upstream         SubResourceClient
}

func (c *consistentSubResourceClient) Get(ctx context.Context, obj, subResource Object, opts ...SubResourceGetOption) error {
	return c.upstream.Get(ctx, obj, subResource, opts...)
}

func (c *consistentSubResourceClient) Create(ctx context.Context, obj, subResource Object, opts ...SubResourceCreateOption) error {
	return c.writeAndRecordRV(ctx, obj, func() error {
		return c.upstream.Create(ctx, obj, subResource, opts...)
	})
}

func (c *consistentSubResourceClient) Update(ctx context.Context, obj Object, opts ...SubResourceUpdateOption) error {
	return c.writeAndRecordRV(ctx, obj, func() error {
		return c.upstream.Update(ctx, obj, opts...)
	})
}

func (c *consistentSubResourceClient) Patch(ctx context.Context, obj Object, patch Patch, opts ...SubResourcePatchOption) error {
	return c.writeAndRecordRV(ctx, obj, func() error {
		return c.upstream.Patch(ctx, obj, patch, opts...)
	})
}

func (c *consistentSubResourceClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...SubResourceApplyOption) error {
	return c.writeAndRecordRV(ctx, obj, func() error {
		return c.upstream.Apply(ctx, obj, opts...)
	})
}

var closedChannel chan struct{}

func init() {
	closedChannel = make(chan struct{})
	close(closedChannel)
}

type writeBatch struct {
	barrier  *keyWriteBarrier
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

// keyWriteBarrier allows to wait for a set of in-flight writes to finish.
type keyWriteBarrier struct {
	// mutex must be held to access current
	mutex   sync.Mutex
	current *writeBatch
}

// Begin adds a write to the current batch, starting one if needed.
func (b *keyWriteBarrier) Begin() func() {
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
func (b *keyWriteBarrier) Seal() <-chan struct{} {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.current == nil {
		return closedChannel
	}

	done := b.current.done
	b.current = nil
	return done
}

func newThreadSafeMap[k comparable, v any](newValue func() v) *threadSafeMap[k, v] {
	return &threadSafeMap[k, v]{
		data:     map[k]v{},
		newValue: newValue,
	}
}

type threadSafeMap[k comparable, v any] struct {
	lock     sync.Mutex
	data     map[k]v
	newValue func() v
}

func (t *threadSafeMap[k, v]) getOrCreate(key k) v {
	t.lock.Lock()
	defer t.lock.Unlock()

	val, exists := t.data[key]
	if !exists {
		val = t.newValue()
		t.data[key] = val
	}

	return val
}

func newWriteBarriers(newBarrier func() writeBarrier) *writeBarriers {
	return &writeBarriers{
		data:       map[types.NamespacedName]*writeBarrierWithRefCounter{},
		newBarrier: newBarrier,
	}
}

type writeBarrierWithRefCounter struct {
	writeBarrier
	inFlightWrites int
}

// writeBarriers holds one writeBarrier per key that has an in-flight write.
type writeBarriers struct {
	lock       sync.Mutex
	data       map[types.NamespacedName]*writeBarrierWithRefCounter
	newBarrier func() writeBarrier
}

func (w *writeBarriers) Begin(key types.NamespacedName) func() {
	w.lock.Lock()
	defer w.lock.Unlock()

	barrier, exists := w.data[key]
	if !exists {
		barrier = &writeBarrierWithRefCounter{writeBarrier: w.newBarrier()}
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

func (w *writeBarriers) seal(key types.NamespacedName) <-chan struct{} {
	w.lock.Lock()
	defer w.lock.Unlock()

	barrier, exists := w.data[key]
	if !exists {
		return closedChannel
	}

	return barrier.Seal()
}

func (w *writeBarriers) sealAll() []<-chan struct{} {
	w.lock.Lock()
	defer w.lock.Unlock()

	result := make([]<-chan struct{}, 0, len(w.data))
	for _, barrier := range w.data {
		result = append(result, barrier.Seal())
	}

	return result
}
