package grpc

import (
	"context"
	"testing"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/storage"
	kverrors "github.com/flowmesh/engine/internal/storage/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockKVStorage is a mock implementation of storage.StorageBackend for testing
type mockKVStorage struct {
	storage.StorageBackend
	kvMgr *mockKVManager
}

func (m *mockKVStorage) KVManager() storage.KVManager {
	return m.kvMgr
}

func (m *mockKVStorage) StreamManager() storage.StreamManager {
	return nil
}

func (m *mockKVStorage) QueueManager() storage.QueueManager {
	return nil
}

func (m *mockKVStorage) ConsumerGroupManager() storage.ConsumerGroupManager {
	return nil
}

func (m *mockKVStorage) Start(ctx context.Context) error {
	return nil
}

func (m *mockKVStorage) Stop(ctx context.Context) error {
	return nil
}

// mockKVManager is a mock implementation of storage.KVManager for testing
type mockKVManager struct {
	setFunc      func(ctx context.Context, resourcePath string, key string, value []byte, options storage.KVSetOptions) error
	getFunc      func(ctx context.Context, resourcePath string, key string) ([]byte, error)
	deleteFunc   func(ctx context.Context, resourcePath string, key string) error
	existsFunc   func(ctx context.Context, resourcePath string, key string) (bool, error)
	listKeysFunc func(ctx context.Context, resourcePath string, prefix string) ([]string, error)
}

func (m *mockKVManager) Set(ctx context.Context, resourcePath string, key string, value []byte, options storage.KVSetOptions) error {
	if m.setFunc != nil {
		return m.setFunc(ctx, resourcePath, key, value, options)
	}
	return nil
}

func (m *mockKVManager) Get(ctx context.Context, resourcePath string, key string) ([]byte, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, resourcePath, key)
	}
	return []byte("test value"), nil
}

func (m *mockKVManager) Delete(ctx context.Context, resourcePath string, key string) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, resourcePath, key)
	}
	return nil
}

func (m *mockKVManager) Exists(ctx context.Context, resourcePath string, key string) (bool, error) {
	if m.existsFunc != nil {
		return m.existsFunc(ctx, resourcePath, key)
	}
	return true, nil
}

func (m *mockKVManager) ListKeys(ctx context.Context, resourcePath string, prefix string) ([]string, error) {
	if m.listKeysFunc != nil {
		return m.listKeysFunc(ctx, resourcePath, prefix)
	}
	return []string{"key1", "key2"}, nil
}

func (m *mockKVManager) Start(ctx context.Context) error {
	return nil
}

func (m *mockKVManager) Stop(ctx context.Context) error {
	return nil
}

func (m *mockKVManager) Ready() bool {
	return true
}

func (m *mockKVManager) InitializeKVStore(ctx context.Context, resourcePath string) error {
	return nil
}

func TestKVService_Set(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockMgr := &mockKVManager{
			setFunc: func(ctx context.Context, resourcePath string, key string, value []byte, options storage.KVSetOptions) error {
				return nil
			},
		}
		mockStorage := &mockKVStorage{kvMgr: mockMgr}
		service := NewKVService(mockStorage)

		req := &flowmeshpb.SetRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "test-kv",
			},
			Key:        "test-key",
			Value:      []byte("test value"),
			TtlSeconds: 0,
		}

		resp, err := service.Set(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
	})

	t.Run("with TTL", func(t *testing.T) {
		mockMgr := &mockKVManager{
			setFunc: func(ctx context.Context, resourcePath string, key string, value []byte, options storage.KVSetOptions) error {
				assert.Equal(t, int64(3600), int64(options.TTL.Seconds()))
				return nil
			},
		}
		mockStorage := &mockKVStorage{kvMgr: mockMgr}
		service := NewKVService(mockStorage)

		req := &flowmeshpb.SetRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "test-kv",
			},
			Key:        "test-key",
			Value:      []byte("test value"),
			TtlSeconds: 3600,
		}

		resp, err := service.Set(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
	})

	t.Run("invalid resource path", func(t *testing.T) {
		service := NewKVService(&mockKVStorage{kvMgr: &mockKVManager{}})

		req := &flowmeshpb.SetRequest{
			ResourcePath: nil,
			Key:          "test-key",
			Value:        []byte("test"),
		}

		_, err := service.Set(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("empty key", func(t *testing.T) {
		service := NewKVService(&mockKVStorage{kvMgr: &mockKVManager{}})

		req := &flowmeshpb.SetRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "test-kv",
			},
			Key:   "",
			Value: []byte("test"),
		}

		_, err := service.Set(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("empty value", func(t *testing.T) {
		service := NewKVService(&mockKVStorage{kvMgr: &mockKVManager{}})

		req := &flowmeshpb.SetRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "test-kv",
			},
			Key:   "test-key",
			Value: []byte{},
		}

		_, err := service.Set(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("KV store not found", func(t *testing.T) {
		mockMgr := &mockKVManager{
			setFunc: func(ctx context.Context, resourcePath string, key string, value []byte, options storage.KVSetOptions) error {
				return kverrors.KVStoreNotFoundError{ResourcePath: resourcePath}
			},
		}
		mockStorage := &mockKVStorage{kvMgr: mockMgr}
		service := NewKVService(mockStorage)

		req := &flowmeshpb.SetRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "nonexistent",
			},
			Key:   "test-key",
			Value: []byte("test"),
		}

		_, err := service.Set(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
	})
}

func TestKVService_Get(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockMgr := &mockKVManager{
			getFunc: func(ctx context.Context, resourcePath string, key string) ([]byte, error) {
				return []byte("test value"), nil
			},
		}
		mockStorage := &mockKVStorage{kvMgr: mockMgr}
		service := NewKVService(mockStorage)

		req := &flowmeshpb.GetRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "test-kv",
			},
			Key: "test-key",
		}

		resp, err := service.Get(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
		assert.Equal(t, []byte("test value"), resp.Value)
	})

	t.Run("key not found", func(t *testing.T) {
		mockMgr := &mockKVManager{
			getFunc: func(ctx context.Context, resourcePath string, key string) ([]byte, error) {
				return nil, kverrors.KeyNotFoundError{ResourcePath: resourcePath, Key: key}
			},
		}
		mockStorage := &mockKVStorage{kvMgr: mockMgr}
		service := NewKVService(mockStorage)

		req := &flowmeshpb.GetRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "test-kv",
			},
			Key: "nonexistent",
		}

		_, err := service.Get(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
	})
}

func TestKVService_Delete(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockMgr := &mockKVManager{
			deleteFunc: func(ctx context.Context, resourcePath string, key string) error {
				return nil
			},
		}
		mockStorage := &mockKVStorage{kvMgr: mockMgr}
		service := NewKVService(mockStorage)

		req := &flowmeshpb.DeleteRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "test-kv",
			},
			Key: "test-key",
		}

		resp, err := service.Delete(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
	})

	t.Run("key not found", func(t *testing.T) {
		mockMgr := &mockKVManager{
			deleteFunc: func(ctx context.Context, resourcePath string, key string) error {
				return kverrors.KeyNotFoundError{ResourcePath: resourcePath, Key: key}
			},
		}
		mockStorage := &mockKVStorage{kvMgr: mockMgr}
		service := NewKVService(mockStorage)

		req := &flowmeshpb.DeleteRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "test-kv",
			},
			Key: "nonexistent",
		}

		_, err := service.Delete(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
	})
}

func TestKVService_Exists(t *testing.T) {
	t.Run("key exists", func(t *testing.T) {
		mockMgr := &mockKVManager{
			existsFunc: func(ctx context.Context, resourcePath string, key string) (bool, error) {
				return true, nil
			},
		}
		mockStorage := &mockKVStorage{kvMgr: mockMgr}
		service := NewKVService(mockStorage)

		req := &flowmeshpb.ExistsRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "test-kv",
			},
			Key: "test-key",
		}

		resp, err := service.Exists(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
		assert.True(t, resp.Exists)
	})

	t.Run("key does not exist", func(t *testing.T) {
		mockMgr := &mockKVManager{
			existsFunc: func(ctx context.Context, resourcePath string, key string) (bool, error) {
				return false, nil
			},
		}
		mockStorage := &mockKVStorage{kvMgr: mockMgr}
		service := NewKVService(mockStorage)

		req := &flowmeshpb.ExistsRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "test-kv",
			},
			Key: "nonexistent",
		}

		resp, err := service.Exists(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
		assert.False(t, resp.Exists)
	})
}

func TestKVService_ListKeys(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockMgr := &mockKVManager{
			listKeysFunc: func(ctx context.Context, resourcePath string, prefix string) ([]string, error) {
				return []string{"key1", "key2", "key3"}, nil
			},
		}
		mockStorage := &mockKVStorage{kvMgr: mockMgr}
		service := NewKVService(mockStorage)

		req := &flowmeshpb.ListKeysRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "test-kv",
			},
			Prefix: "",
		}

		resp, err := service.ListKeys(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
		assert.Len(t, resp.Keys, 3)
	})

	t.Run("with prefix", func(t *testing.T) {
		mockMgr := &mockKVManager{
			listKeysFunc: func(ctx context.Context, resourcePath string, prefix string) ([]string, error) {
				assert.Equal(t, "user:", prefix)
				return []string{"user:1", "user:2"}, nil
			},
		}
		mockStorage := &mockKVStorage{kvMgr: mockMgr}
		service := NewKVService(mockStorage)

		req := &flowmeshpb.ListKeysRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "test-kv",
			},
			Prefix: "user:",
		}

		resp, err := service.ListKeys(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, int32(codes.OK), resp.Status.Code)
		assert.Len(t, resp.Keys, 2)
	})

	t.Run("KV store not found", func(t *testing.T) {
		mockMgr := &mockKVManager{
			listKeysFunc: func(ctx context.Context, resourcePath string, prefix string) ([]string, error) {
				return nil, kverrors.KVStoreNotFoundError{ResourcePath: resourcePath}
			},
		}
		mockStorage := &mockKVStorage{kvMgr: mockMgr}
		service := NewKVService(mockStorage)

		req := &flowmeshpb.ListKeysRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "nonexistent",
			},
		}

		_, err := service.ListKeys(context.Background(), req)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
	})
}
