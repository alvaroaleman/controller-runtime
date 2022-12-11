package apiutil

import (
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
)

type LazyRestMapperOpts struct {
	NewUpstream func() (meta.RESTMapper, error)
}

func NewLazyRestMapper(cfg *rest.Config, o LazyRestMapperOpts) (meta.RESTMapper, error) {
	c, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, err
	}
	if o.NewUpstream == nil {
		//		o.NewUpstream = meta.
	}
	return &lazyRestMapper{
		c:           c,
		cache:       map[schema.GroupVersion][]metav1.APIResource{},
		newUpstream: o.NewUpstream,
	}, nil
}

type lazyRestMapper struct {
	c *discovery.DiscoveryClient

	cache     map[schema.GroupVersion][]metav1.APIResource
	cacheLock sync.RWMutex

	newUpstream  func() (meta.RESTMapper, error)
	upstream     meta.RESTMapper
	upstreamLock sync.Mutex
}

func (l *lazyRestMapper) getUpstream() (meta.RESTMapper, error) {
	l.upstreamLock.Lock()
	defer l.upstreamLock.Unlock()

	if l.upstream != nil {
		return l.upstream, nil
	}

	upstream, err := l.newUpstream()
	if err != nil {
		return nil, err
	}

	l.upstream = upstream
	return l.upstream, nil
}

func (l *lazyRestMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	upstream, err := l.getUpstream()
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	return upstream.KindFor(resource)
}

func (l *lazyRestMapper) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	upstream, err := l.getUpstream()
	if err != nil {
		return nil, err
	}
	return upstream.KindsFor(resource)
}

func (l *lazyRestMapper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	upstream, err := l.getUpstream()
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	return upstream.ResourceFor(input)
}

func (l *lazyRestMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	upstream, err := l.getUpstream()
	if err != nil {
		return nil, err
	}
	return upstream.ResourcesFor(input)
}

func (l *lazyRestMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	upstream, err := l.getUpstream()
	if err != nil {
		return nil, err
	}
	return upstream.RESTMappings(gk, versions...)
}

func (l *lazyRestMapper) ResourceSingularizer(resource string) (singular string, err error) {
	upstream, err := l.getUpstream()
	if err != nil {
		return "", err
	}
	return upstream.ResourceSingularizer(resource)
}

func (l *lazyRestMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if !l.canProvideMapping(gk, versions...) {
		upstream, err := l.getUpstream()
		if err != nil {
			return nil, err
		}
		return upstream.RESTMapping(gk, versions...)
	}

	gvk := gk.WithVersion(versions[0])
	apiResources, err := l.getAPIResources(gvk)
	if err != nil {
		return nil, err
	}

	for _, resource := range apiResources {
		if resource.Kind == gk.Kind {
			return &meta.RESTMapping{
				Resource: schema.GroupVersionResource{
					Group:    gk.Group,
					Version:  versions[0],
					Resource: resource.Name,
				},
				GroupVersionKind: schema.GroupVersionKind{
					Group:   gk.Group,
					Version: versions[0],
					Kind:    gk.Kind,
				},
				Scope: &restScpope{namespaced: resource.Namespaced},
			}, nil
		}
	}

	return nil, &meta.NoKindMatchError{GroupKind: gk, SearchedVersions: versions}
}

func (l *lazyRestMapper) canProvideMapping(gk schema.GroupKind, versions ...string) bool {
	if len(versions) != 1 {
		return false
	}

	if versions[0] == "v1" && gk.Kind != "" {
		return true
	}

	return gk.Group != "" && gk.Kind != "" && versions[0] != ""
}

func (l *lazyRestMapper) getAPIResources(gvk schema.GroupVersionKind) ([]metav1.APIResource, error) {
	l.cacheLock.RLock()
	apiResources, found := l.cache[gvk.GroupVersion()]
	l.cacheLock.RUnlock()
	if found {
		return apiResources, nil
	}

	l.cacheLock.Lock()
	defer l.cacheLock.Unlock()

	// Check again in case it was retrieved while we waited for the lock
	apiResources, found = l.cache[gvk.GroupVersion()]
	if found {
		return apiResources, nil
	}

	resources, err := l.c.ServerResourcesForGroupVersion(gvk.GroupVersion().String())
	if err != nil {
		// TODO: Meta npo match error?
		return nil, fmt.Errorf("failed to get resources for groupVersion %s: %w", gvk.GroupVersion().String(), err)
	}

	l.cache[gvk.GroupVersion()] = resources.APIResources
	return resources.APIResources, nil
}

type restScpope struct {
	namespaced bool
}

func (r *restScpope) Name() meta.RESTScopeName {
	if r.namespaced {
		return meta.RESTScopeNameNamespace
	}
	return meta.RESTScopeNameRoot
}
