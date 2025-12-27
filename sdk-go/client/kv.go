package client

import (
	"context"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
)

// KVClient provides operations for working with KV stores
type KVClient struct {
	client *Client
}

// Set sets a key-value pair with optional TTL
func (k *KVClient) Set(ctx context.Context, tenant, namespace, store, key string, value []byte, opts ...KVSetOption) error {
	options := &KVSetOptions{}
	for _, opt := range opts {
		opt(options)
	}

	req := &flowmeshpb.SetRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "kv", store),
		Key:          key,
		Value:        value,
		TtlSeconds:   int64(options.TTL.Seconds()),
	}

	_, err := k.client.kvClient.Set(k.client.withAuth(ctx), req)
	return wrapError(err, "set")
}

// Get retrieves a value by key
func (k *KVClient) Get(ctx context.Context, tenant, namespace, store, key string) ([]byte, error) {
	req := &flowmeshpb.GetRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "kv", store),
		Key:          key,
	}

	resp, err := k.client.kvClient.Get(k.client.withAuth(ctx), req)
	if err != nil {
		return nil, wrapError(err, "get")
	}

	return resp.Value, nil
}

// Delete deletes a key
func (k *KVClient) Delete(ctx context.Context, tenant, namespace, store, key string) error {
	req := &flowmeshpb.DeleteRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "kv", store),
		Key:          key,
	}

	_, err := k.client.kvClient.Delete(k.client.withAuth(ctx), req)
	return wrapError(err, "delete")
}

// Exists checks if a key exists
func (k *KVClient) Exists(ctx context.Context, tenant, namespace, store, key string) (bool, error) {
	req := &flowmeshpb.ExistsRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "kv", store),
		Key:          key,
	}

	resp, err := k.client.kvClient.Exists(k.client.withAuth(ctx), req)
	if err != nil {
		return false, wrapError(err, "exists")
	}

	return resp.Exists, nil
}

// ListKeys lists all keys, optionally filtered by prefix
func (k *KVClient) ListKeys(ctx context.Context, tenant, namespace, store, prefix string) ([]string, error) {
	req := &flowmeshpb.ListKeysRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "kv", store),
		Prefix:       prefix,
	}

	resp, err := k.client.kvClient.ListKeys(k.client.withAuth(ctx), req)
	if err != nil {
		return nil, wrapError(err, "list keys")
	}

	return resp.Keys, nil
}
