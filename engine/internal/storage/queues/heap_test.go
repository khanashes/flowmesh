package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobMinHeap_PushAndPop(t *testing.T) {
	heap := NewJobMinHeap()

	now := time.Now()

	// Push jobs with different VisibleAt times
	job1 := &JobMetadata{
		ID:        "job1",
		VisibleAt: now.Add(10 * time.Second),
	}
	job2 := &JobMetadata{
		ID:        "job2",
		VisibleAt: now.Add(5 * time.Second),
	}
	job3 := &JobMetadata{
		ID:        "job3",
		VisibleAt: now,
	}

	heap.PushJob(job1)
	heap.PushJob(job2)
	heap.PushJob(job3)

	// Pop should return jobs in order of VisibleAt (soonest first)
	popped1 := heap.PopJob()
	require.NotNil(t, popped1)
	assert.Equal(t, "job3", popped1.ID) // Soonest

	popped2 := heap.PopJob()
	require.NotNil(t, popped2)
	assert.Equal(t, "job2", popped2.ID)

	popped3 := heap.PopJob()
	require.NotNil(t, popped3)
	assert.Equal(t, "job1", popped3.ID)

	// Heap should be empty now
	assert.Nil(t, heap.PopJob())
}

func TestJobMinHeap_Peek(t *testing.T) {
	heap := NewJobMinHeap()

	now := time.Now()

	job1 := &JobMetadata{
		ID:        "job1",
		VisibleAt: now.Add(10 * time.Second),
	}
	job2 := &JobMetadata{
		ID:        "job2",
		VisibleAt: now,
	}

	heap.PushJob(job1)
	heap.PushJob(job2)

	// Peek should return soonest without removing
	peeked := heap.Peek()
	require.NotNil(t, peeked)
	assert.Equal(t, "job2", peeked.ID)

	// Peek again should return same job
	peeked2 := heap.Peek()
	assert.Equal(t, "job2", peeked2.ID)

	// Pop should still work
	popped := heap.PopJob()
	assert.Equal(t, "job2", popped.ID)
}

func TestJobMinHeap_Remove(t *testing.T) {
	heap := NewJobMinHeap()

	now := time.Now()

	job1 := &JobMetadata{
		ID:        "job1",
		VisibleAt: now,
	}
	job2 := &JobMetadata{
		ID:        "job2",
		VisibleAt: now.Add(5 * time.Second),
	}

	heap.PushJob(job1)
	heap.PushJob(job2)

	// Remove job1
	removed := heap.Remove("job1")
	assert.True(t, removed)

	// Pop should return job2
	popped := heap.PopJob()
	assert.Equal(t, "job2", popped.ID)

	// Try to remove non-existent job
	removed = heap.Remove("nonexistent")
	assert.False(t, removed)
}

func TestJobMinHeap_Len(t *testing.T) {
	heap := NewJobMinHeap()

	assert.Equal(t, 0, heap.Len())

	now := time.Now()
	heap.PushJob(&JobMetadata{ID: "job1", VisibleAt: now})
	assert.Equal(t, 1, heap.Len())

	heap.PushJob(&JobMetadata{ID: "job2", VisibleAt: now})
	assert.Equal(t, 2, heap.Len())

	heap.PopJob()
	assert.Equal(t, 1, heap.Len())
}
