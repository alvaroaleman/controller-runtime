/*
Copyright 2018 The Kubernetes Authors.

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

package source

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	internal "sigs.k8s.io/controller-runtime/pkg/internal/source"

	"sigs.k8s.io/controller-runtime/pkg/cache"
)

// Source is a source of events (e.g. Create, Update, Delete operations on Kubernetes Objects, Webhook callbacks, etc)
// which should be processed by event.EventHandlers to enqueue reconcile.Requests.
//
// * Use Kind for events originating in the cluster (e.g. Pod Create, Pod Update, Deployment Update).
//
// * Use Channel for events originating outside the cluster (e.g. GitHub Webhook callback, Polling external urls).
//
// Users may build their own Source implementations.
type Source interface {
	// Start is internal and should be called only by the Controller to register an EventHandler with the Informer
	// to enqueue reconcile.Requests.
	Start(context.Context, workqueue.RateLimitingInterface) error
}

// SyncingSource is a source that needs syncing prior to being usable. The controller
// will call its WaitForSync prior to starting workers.
type SyncingSource interface {
	Source
	WaitForSync(ctx context.Context) error
}

// Kind creates a KindSource with the given cache provider.
func Kind[T client.Object](cache cache.Cache, object T, handler handler.TypedEventHandler[T]) SyncingSource {
	return &internal.Kind[T]{
		Type:    object,
		Cache:   cache,
		Handler: handler,
	}
}

var _ Source = &channel[string]{}

type channelOptions struct {
	DestBufferSize int
}

// ChannelOption is a functional option for configuring a Channel source.
type ChannelOption func(*channelOptions)

// WithDestBufferSize specifies the buffer size of dest channels.
func WithDestBufferSize(destBufferSize int) ChannelOption {
	return func(o *channelOptions) {
		if destBufferSize <= 0 {
			return // ignore invalid buffer size
		}

		o.DestBufferSize = destBufferSize
	}
}

// Channel is used to provide a source of events originating outside the cluster
// (e.g. GitHub Webhook callback).  Channel requires the user to wire the external
// source (e.g. http handler) to write GenericEvents to the underlying channel.
func Channel[T any](source <-chan event.TypedGenericEvent[T], handler handler.TypedEventHandler[T], options ...ChannelOption) Source {
	opts := channelOptions{
		// 1024 is the default number of event notifications that can be buffered.
		DestBufferSize: 1024,
	}
	for _, o := range options {
		if o == nil {
			continue // ignore nil options
		}
		o(&opts)
	}

	return &channel[T]{
		options: opts,
		source:  source,
		handler: handler,
	}
}

type channel[T any] struct {
	// once ensures the event distribution goroutine will be performed only once
	once sync.Once

	// source is the source channel to fetch GenericEvents
	source <-chan event.TypedGenericEvent[T]

	handler handler.TypedEventHandler[T]

	options channelOptions

	// dest is the destination channels of the added event handlers
	dest []chan event.TypedGenericEvent[T]

	// destLock is to ensure the destination channels are safely added/removed
	destLock sync.Mutex
}

func (cs *channel[T]) String() string {
	return fmt.Sprintf("channel source: %p", cs)
}

// Start implements Source and should only be called by the Controller.
func (cs *channel[T]) Start(
	ctx context.Context,
	queue workqueue.RateLimitingInterface,
) error {
	// Source should have been specified by the user.
	if cs.source == nil {
		return fmt.Errorf("must specify Channel.Source")
	}
	if cs.handler == nil {
		return errors.New("must specify Channel.Handler")
	}

	dst := make(chan event.TypedGenericEvent[T], cs.options.DestBufferSize)

	cs.destLock.Lock()
	cs.dest = append(cs.dest, dst)
	cs.destLock.Unlock()

	cs.once.Do(func() {
		// Distribute GenericEvents to all EventHandler / Queue pairs Watching this source
		go cs.syncLoop(ctx)
	})

	go func() {
		for evt := range dst {
			func() {
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				cs.handler.Generic(ctx, evt, queue)
			}()
		}
	}()

	return nil
}

func (cs *channel[T]) doStop() {
	cs.destLock.Lock()
	defer cs.destLock.Unlock()

	for _, dst := range cs.dest {
		close(dst)
	}
}

func (cs *channel[T]) distribute(evt event.TypedGenericEvent[T]) {
	cs.destLock.Lock()
	defer cs.destLock.Unlock()

	for _, dst := range cs.dest {
		// We cannot make it under goroutine here, or we'll meet the
		// race condition of writing message to closed channels.
		// To avoid blocking, the dest channels are expected to be of
		// proper buffer size. If we still see it blocked, then
		// the controller is thought to be in an abnormal state.
		dst <- evt
	}
}

func (cs *channel[T]) syncLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Close destination channels
			cs.doStop()
			return
		case evt, stillOpen := <-cs.source:
			if !stillOpen {
				// if the source channel is closed, we're never gonna get
				// anything more on it, so stop & bail
				cs.doStop()
				return
			}
			cs.distribute(evt)
		}
	}
}

// informer is used to provide a source of events originating inside the cluster from Watches (e.g. Pod Create).
type informer struct {
	// Informer is the controller-runtime Informer
	Informer cache.Informer
	Handler  handler.EventHandler
}

var _ Source = &informer{}

// Informer creates an InformerSource with the given cache provider.
func Informer(inf cache.Informer, eventhandler handler.EventHandler) Source {
	return &informer{Informer: inf, Handler: eventhandler}
}

// Start is internal and should be called only by the Controller to register an EventHandler with the Informer
// to enqueue reconcile.Requests.
func (is *informer) Start(ctx context.Context, queue workqueue.RateLimitingInterface) error {
	// Informer should have been specified by the user.
	if is.Informer == nil {
		return fmt.Errorf("must specify Informer.Informer")
	}
	if is.Handler == nil {
		return errors.New("must specify Informer.Handler")
	}

	_, err := is.Informer.AddEventHandler(internal.NewEventHandler(ctx, queue, is.Handler).HandlerFuncs())
	if err != nil {
		return err
	}
	return nil
}

func (is *informer) String() string {
	return fmt.Sprintf("informer source: %p", is.Informer)
}

var _ Source = Func(nil)

// Func is a function that implements Source.
type Func func(context.Context, workqueue.RateLimitingInterface) error

// Start implements Source.
func (f Func) Start(ctx context.Context, queue workqueue.RateLimitingInterface) error {
	return f(ctx, queue)
}

func (f Func) String() string {
	return fmt.Sprintf("func source: %p", f)
}
