package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/flowmesh/engine/internal/storage/metastore"
)

// ResourceFactory provides a factory pattern for creating resources
type ResourceFactory struct {
	metaStore *metastore.Store
}

// NewResourceFactory creates a new resource factory
func NewResourceFactory(metaStore *metastore.Store) *ResourceFactory {
	return &ResourceFactory{
		metaStore: metaStore,
	}
}

// CreateStream creates a new stream resource
func (f *ResourceFactory) CreateStream(ctx context.Context, tenant, namespace, name string, partitions int32) (*metastore.ResourceConfig, error) {
	config := &metastore.ResourceConfig{
		Tenant:     tenant,
		Namespace:  namespace,
		Name:       name,
		Type:       metastore.ResourceStream,
		Partitions: int(partitions),
		CreatedAt:  time.Now(),
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid stream config: %w", err)
	}

	if err := f.metaStore.CreateResource(config); err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	return config, nil
}

// CreateQueue creates a new queue resource
func (f *ResourceFactory) CreateQueue(ctx context.Context, tenant, namespace, name string, partitions int32) (*metastore.ResourceConfig, error) {
	config := &metastore.ResourceConfig{
		Tenant:     tenant,
		Namespace:  namespace,
		Name:       name,
		Type:       metastore.ResourceQueue,
		Partitions: int(partitions),
		CreatedAt:  time.Now(),
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid queue config: %w", err)
	}

	if err := f.metaStore.CreateResource(config); err != nil {
		return nil, fmt.Errorf("failed to create queue: %w", err)
	}

	return config, nil
}

// CreateKVStore creates a new KV store resource
func (f *ResourceFactory) CreateKVStore(ctx context.Context, tenant, namespace, name string) (*metastore.ResourceConfig, error) {
	config := &metastore.ResourceConfig{
		Tenant:    tenant,
		Namespace: namespace,
		Name:      name,
		Type:      metastore.ResourceKV,
		CreatedAt: time.Now(),
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid KV store config: %w", err)
	}

	if err := f.metaStore.CreateResource(config); err != nil {
		return nil, fmt.Errorf("failed to create KV store: %w", err)
	}

	return config, nil
}

// GetResource retrieves a resource by path
func (f *ResourceFactory) GetResource(ctx context.Context, resourcePath string) (*metastore.ResourceConfig, error) {
	return f.metaStore.GetResource(resourcePath)
}

// DeleteResource deletes a resource
func (f *ResourceFactory) DeleteResource(ctx context.Context, resourcePath string) error {
	return f.metaStore.DeleteResource(resourcePath)
}

// ListResources lists all resources, optionally filtered by tenant, namespace, and type
// Empty strings for tenant/namespace mean "all"
func (f *ResourceFactory) ListResources(ctx context.Context, tenant, namespace string, resourceType metastore.ResourceType) ([]*metastore.ResourceConfig, error) {
	return f.metaStore.ListResources(tenant, namespace, resourceType)
}
