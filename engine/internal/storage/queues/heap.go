package queues

import (
	"container/heap"
	"sync"
)

// Ensure JobMinHeap implements heap.Interface
var _ heap.Interface = (*JobMinHeap)(nil)

// JobMinHeap is a min-heap of jobs ordered by VisibleAt (soonest first)
type JobMinHeap struct {
	jobs []*JobMetadata
	mu   sync.RWMutex
}

// NewJobMinHeap creates a new job min-heap
func NewJobMinHeap() *JobMinHeap {
	h := &JobMinHeap{
		jobs: make([]*JobMetadata, 0),
	}
	heap.Init(h)
	return h
}

// Len returns the number of jobs in the heap
// Note: heap.Interface methods should NOT lock - they're called by heap package
func (h *JobMinHeap) Len() int {
	return len(h.jobs)
}

// Less compares two jobs by VisibleAt (min-heap: soonest first)
// Note: heap.Interface methods should NOT lock - they're called by heap package
func (h *JobMinHeap) Less(i, j int) bool {
	return h.jobs[i].VisibleAt.Before(h.jobs[j].VisibleAt)
}

// Swap swaps two jobs in the heap
// Note: heap.Interface methods should NOT lock - they're called by heap package
func (h *JobMinHeap) Swap(i, j int) {
	h.jobs[i], h.jobs[j] = h.jobs[j], h.jobs[i]
}

// Push adds a job to the heap (implements heap.Interface)
// Note: heap.Interface methods should NOT lock - they're called by heap package
func (h *JobMinHeap) Push(x interface{}) {
	h.jobs = append(h.jobs, x.(*JobMetadata))
}

// Pop removes and returns the job with the earliest VisibleAt (implements heap.Interface)
// Note: heap.Interface methods should NOT lock - they're called by heap package
func (h *JobMinHeap) Pop() interface{} {
	old := h.jobs
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	h.jobs = old[0 : n-1]
	return item
}

// PushJob adds a job to the heap (thread-safe wrapper)
func (h *JobMinHeap) PushJob(job *JobMetadata) {
	h.mu.Lock()
	defer h.mu.Unlock()
	heap.Push(h, job)
}

// PopJob removes and returns the job with the earliest VisibleAt (thread-safe wrapper)
func (h *JobMinHeap) PopJob() *JobMetadata {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.jobs) == 0 {
		return nil
	}
	return heap.Pop(h).(*JobMetadata)
}

// Peek returns the job with the earliest VisibleAt without removing it
func (h *JobMinHeap) Peek() *JobMetadata {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.jobs) == 0 {
		return nil
	}
	return h.jobs[0]
}

// Remove removes a specific job by ID from the heap
func (h *JobMinHeap) Remove(jobID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i, job := range h.jobs {
		if job.ID == jobID {
			heap.Remove(h, i)
			return true
		}
	}
	return false
}

// Copy creates a copy of the heap for safe iteration
func (h *JobMinHeap) Copy() *JobMinHeap {
	h.mu.RLock()
	defer h.mu.RUnlock()

	copy := &JobMinHeap{
		jobs: make([]*JobMetadata, len(h.jobs)),
	}
	for i, job := range h.jobs {
		// Create a shallow copy of the job
		jobCopy := *job
		copy.jobs[i] = &jobCopy
	}
	heap.Init(copy)
	return copy
}

// GetAll returns all jobs in the heap (for testing/recovery)
func (h *JobMinHeap) GetAll() []*JobMetadata {
	h.mu.RLock()
	defer h.mu.RUnlock()
	result := make([]*JobMetadata, len(h.jobs))
	copy(result, h.jobs)
	return result
}
