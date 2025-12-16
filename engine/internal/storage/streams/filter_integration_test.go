package streams

import (
	"context"
	"testing"

	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManager_ReadWithOptions_Filter(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a stream resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "filter-stream",
		Type:       metastore.ResourceStream,
		Partitions: 1,
	}
	require.NoError(t, config.Validate())
	require.NoError(t, metaStore.CreateResource(config))

	resourcePath := config.GetPath()
	require.NoError(t, manager.InitializeStream(resourcePath))

	// Write events with headers
	events := []Event{
		{Payload: []byte("order1"), Headers: map[string]string{"type": "order", "region": "us"}},
		{Payload: []byte("payment1"), Headers: map[string]string{"type": "payment", "region": "us"}},
		{Payload: []byte("order2"), Headers: map[string]string{"type": "order", "region": "eu"}},
		{Payload: []byte("login1"), Headers: map[string]string{"type": "login", "region": "us"}},
	}

	_, err := manager.WriteEvents(context.Background(), resourcePath, events)
	require.NoError(t, err)

	// Test 1: Filter by exact match
	messages, err := manager.ReadWithOptions(context.Background(), resourcePath, 0, ReadOptions{
		Offset:      0,
		MaxMessages: 10,
		Filter:      `headers.type == "order"`,
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(messages))
	assert.Equal(t, []byte("order1"), messages[0].Payload)
	assert.Equal(t, []byte("order2"), messages[1].Payload)

	// Test 2: Filter by inequality
	messages, err = manager.ReadWithOptions(context.Background(), resourcePath, 0, ReadOptions{
		Offset:      0,
		MaxMessages: 10,
		Filter:      `headers.type != "payment"`,
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(messages)) // order1, order2, login1

	// Test 3: Filter with logical AND
	messages, err = manager.ReadWithOptions(context.Background(), resourcePath, 0, ReadOptions{
		Offset:      0,
		MaxMessages: 10,
		Filter:      `headers.type == "order" && headers.region == "us"`,
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(messages))
	assert.Equal(t, []byte("order1"), messages[0].Payload)

	// Test 4: Filter with logical OR
	messages, err = manager.ReadWithOptions(context.Background(), resourcePath, 0, ReadOptions{
		Offset:      0,
		MaxMessages: 10,
		Filter:      `headers.type == "payment" || headers.region == "eu"`,
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(messages)) // payment1 (us), order2 (eu)
	assert.Equal(t, []byte("payment1"), messages[0].Payload)
	assert.Equal(t, []byte("order2"), messages[1].Payload)
}
