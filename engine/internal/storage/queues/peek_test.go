package queues

import (
	"context"
	"testing"
	"time"

	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManager_Peek_Filter(t *testing.T) {
	manager, metaStore, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create a queue resource
	config := &metastore.ResourceConfig{
		Tenant:     "test-tenant",
		Namespace:  "test-ns",
		Name:       "peek-queue",
		Type:       metastore.ResourceQueue,
		Partitions: 1,
	}
	require.NoError(t, config.Validate())
	require.NoError(t, metaStore.CreateResource(config))

	resourcePath := config.GetPath()
	require.NoError(t, manager.InitializeQueue(resourcePath))

	// Enqueue jobs
	// Job 1: type=email, priority=high
	_, _, err := manager.Enqueue(context.Background(), resourcePath, []byte("email1"), EnqueueOptions{
		Headers: map[string]string{"type": "email", "priority": "high"},
	})
	require.NoError(t, err)

	// Job 2: type=sms, priority=low
	_, _, err = manager.Enqueue(context.Background(), resourcePath, []byte("sms1"), EnqueueOptions{
		Headers: map[string]string{"type": "sms", "priority": "low"},
	})
	require.NoError(t, err)

	// Job 3: type=email, priority=low
	_, _, err = manager.Enqueue(context.Background(), resourcePath, []byte("email2"), EnqueueOptions{
		Headers: map[string]string{"type": "email", "priority": "low"},
	})
	require.NoError(t, err)

	// Test 1: Peek all (limit 10)
	messages, err := manager.Peek(context.Background(), resourcePath, "", 10)
	require.NoError(t, err)
	require.Equal(t, 3, len(messages))
	// Order should be sequential (0, 1, 2) since visibleAt is same (now)

	// Test 2: Filter type == email
	messages, err = manager.Peek(context.Background(), resourcePath, `headers.type == "email"`, 10)
	require.NoError(t, err)
	require.Equal(t, 2, len(messages))
	assert.Equal(t, []byte("email1"), messages[0].Payload)
	assert.Equal(t, []byte("email2"), messages[1].Payload)

	// Test 3: Filter priority == high
	messages, err = manager.Peek(context.Background(), resourcePath, `headers.priority == "high"`, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(messages))
	assert.Equal(t, []byte("email1"), messages[0].Payload)

	// Test 4: Filter with limit
	messages, err = manager.Peek(context.Background(), resourcePath, `headers.type == "email"`, 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(messages))
	assert.Equal(t, []byte("email1"), messages[0].Payload)

	// Test 5: Filter delayed job (should be peekable even if not ready? No, Peek iterates ReadyHeap)
	// ReadyHeap contains visible and invisible jobs? No, ReadyHeap is min-heap of ALL jobs.
	// Wait, Check `Enqueue`. `state.ReadyHeap.PushJob(jobMetadata)`. It pushes ALL jobs.
	// `PopJob` checks `if job.VisibleAt.After(now) return nil`.
	// My `Peek` iterates `ReadyHeap` and sorts by `VisibleAt`.
	// So delayed jobs ARE in the heap and WILL be returned by `Peek` if they match filter (unless I filter by visible_at).

	// Enqueue delayed job
	_, _, err = manager.Enqueue(context.Background(), resourcePath, []byte("future"), EnqueueOptions{
		Delay:   1 * time.Hour,
		Headers: map[string]string{"type": "scheduled"},
	})
	require.NoError(t, err)

	// Peek scheduled
	messages, err = manager.Peek(context.Background(), resourcePath, `headers.type == "scheduled"`, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(messages))
	assert.Equal(t, []byte("future"), messages[0].Payload)
}
