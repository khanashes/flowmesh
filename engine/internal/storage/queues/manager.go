package queues

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/rs/zerolog"
)

// QueueState represents the state of a queue
type QueueState struct {
	ResourcePath string
	NextSeq      int64
	ReadyHeap    *JobMinHeap
	InFlight     map[string]*JobMetadata
	mu           sync.RWMutex
}

// Manager manages queue operations
type Manager struct {
	metaStore   *metastore.Store
	logManager  *log.Manager
	metadataDir string
	queueStates map[string]*QueueState
	log         zerolog.Logger
	mu          sync.RWMutex
}

// NewManager creates a new queue manager
func NewManager(metaStore *metastore.Store, logManager *log.Manager, metadataDir string) *Manager {
	return &Manager{
		metaStore:   metaStore,
		logManager:  logManager,
		metadataDir: metadataDir,
		queueStates: make(map[string]*QueueState),
		log:         logger.WithComponent("queues"),
	}
}

// getOrCreateQueueState gets or creates queue state for a resource
func (m *Manager) getOrCreateQueueState(resourcePath string) *QueueState {
	m.mu.Lock()
	defer m.mu.Unlock()

	if state, exists := m.queueStates[resourcePath]; exists {
		return state
	}

	state := &QueueState{
		ResourcePath: resourcePath,
		NextSeq:      0,
		ReadyHeap:    NewJobMinHeap(),
		InFlight:     make(map[string]*JobMetadata),
	}
	m.queueStates[resourcePath] = state
	return state
}

// Enqueue enqueues a job to a queue and returns job ID and sequence number
func (m *Manager) Enqueue(resourcePath string, payload []byte, options EnqueueOptions) (string, int64, error) {
	// Validate queue exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return "", 0, QueueNotFoundError{ResourcePath: resourcePath}
		}
		return "", 0, EnqueueError{ResourcePath: resourcePath, Err: err}
	}

	// Get or create queue state
	state := m.getOrCreateQueueState(resourcePath)

	// Partition 0 for MVP
	partition := int32(0)

	state.mu.Lock()
	defer state.mu.Unlock()

	// Assign next sequence number
	seq := state.NextSeq
	state.NextSeq++

	// Generate unique job ID
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return "", 0, EnqueueError{ResourcePath: resourcePath, Err: fmt.Errorf("failed to generate ID: %w", err)}
	}
	jobID := hex.EncodeToString(idBytes)

	// Calculate VisibleAt
	now := time.Now()
	visibleAt := now.Add(options.Delay)

	// Get active segment writer
	writer, err := m.logManager.OpenSegment(resourcePath, partition)
	if err != nil {
		return "", 0, EnqueueError{ResourcePath: resourcePath, Err: err}
	}

	// Get file position before writing
	fileOffset := writer.Offset()

	// Create message
	msg := &log.Message{
		ID:            jobID,
		ResourcePath:  resourcePath,
		Partition:     partition,
		Offset:        0, // Not used for queues
		Seq:           seq,
		Type:          log.MessageTypeQueue,
		Payload:       payload,
		Headers:       options.Headers,
		CreatedAt:     now,
		VisibleAt:     visibleAt,
		Attempts:      0,
		SchemaVersion: 0, // Future: schema versioning
	}

	// Encode message
	encoded, err := log.EncodeMessage(msg)
	if err != nil {
		return "", 0, EnqueueError{ResourcePath: resourcePath, Err: fmt.Errorf("failed to encode message: %w", err)}
	}

	// Write to segment
	if err := writer.WriteEntry(encoded); err != nil {
		return "", 0, EnqueueError{ResourcePath: resourcePath, Err: fmt.Errorf("failed to write entry: %w", err)}
	}

	// Get segment path for PayloadPos
	segments, err := m.logManager.ListSegments(resourcePath, partition)
	if err == nil && len(segments) > 0 {
		// Use the last segment (most recent)
		lastSegment := segments[len(segments)-1]

		// Create job metadata
		jobMetadata := &JobMetadata{
			ID:           jobID,
			Seq:          seq,
			VisibleAt:    visibleAt,
			ReserveUntil: time.Time{}, // Set when reserved
			PayloadPos: log.FilePointer{
				File:   lastSegment.Path,
				Offset: fileOffset,
			},
			Attempts:  0,
			CreatedAt: now,
		}

		// Push job into ReadyHeap
		state.ReadyHeap.PushJob(jobMetadata)
	}

	// Flush segment
	if err := writer.Flush(); err != nil {
		m.log.Warn().Err(err).Str("resource", resourcePath).Msg("Failed to flush segment after enqueue")
	}

	m.log.Debug().
		Str("resource", resourcePath).
		Str("job_id", jobID).
		Int64("seq", seq).
		Time("visible_at", visibleAt).
		Msg("Job enqueued")

	return jobID, seq, nil
}

// AddToInFlight moves a job from ReadyHeap to InFlight
func (m *Manager) AddToInFlight(resourcePath string, jobID string, visibilityTimeout time.Duration) (*JobMetadata, error) {
	// Validate queue exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, QueueNotFoundError{ResourcePath: resourcePath}
		}
		return nil, err
	}

	// Validate visibility timeout
	if visibilityTimeout <= 0 {
		return nil, InvalidVisibilityTimeoutError{
			Timeout: visibilityTimeout,
			Reason:  "must be greater than zero",
		}
	}

	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return nil, QueueNotFoundError{ResourcePath: resourcePath}
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	// Check if already in flight
	if job, exists := state.InFlight[jobID]; exists {
		return job, nil
	}

	// Find job in ReadyHeap
	var job *JobMetadata
	allJobs := state.ReadyHeap.GetAll()
	for _, j := range allJobs {
		if j.ID == jobID {
			job = j
			break
		}
	}

	if job == nil {
		return nil, JobNotFoundError{JobID: jobID, ResourcePath: resourcePath}
	}

	// Check if job is visible
	now := time.Now()
	if job.VisibleAt.After(now) {
		return nil, fmt.Errorf("job %s is not yet visible (visible at %v)", jobID, job.VisibleAt) //nolint:goerr113
	}

	// Remove from ReadyHeap
	state.ReadyHeap.Remove(jobID)

	// Set ReserveUntil
	job.ReserveUntil = now.Add(visibilityTimeout)
	job.Attempts++

	// Add to InFlight
	state.InFlight[jobID] = job

	m.log.Debug().
		Str("resource", resourcePath).
		Str("job_id", jobID).
		Time("reserve_until", job.ReserveUntil).
		Msg("Job moved to InFlight")

	return job, nil
}

// RemoveFromInFlight removes a job from InFlight (used for ACK)
func (m *Manager) RemoveFromInFlight(resourcePath string, jobID string) error {
	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return QueueNotFoundError{ResourcePath: resourcePath}
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	// Remove from InFlight
	if _, exists := state.InFlight[jobID]; !exists {
		return JobNotFoundError{JobID: jobID, ResourcePath: resourcePath}
	}

	delete(state.InFlight, jobID)

	m.log.Debug().
		Str("resource", resourcePath).
		Str("job_id", jobID).
		Msg("Job removed from InFlight (ACKed)")

	return nil
}

// GetInFlight retrieves a job from InFlight
func (m *Manager) GetInFlight(resourcePath string, jobID string) (*JobMetadata, error) {
	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return nil, QueueNotFoundError{ResourcePath: resourcePath}
	}

	state.mu.RLock()
	defer state.mu.RUnlock()

	job, exists := state.InFlight[jobID]
	if !exists {
		return nil, JobNotFoundError{JobID: jobID, ResourcePath: resourcePath}
	}

	return job, nil
}

// PopReadyJob pops a ready job from the heap (if visible)
func (m *Manager) PopReadyJob(resourcePath string) (*JobMetadata, error) {
	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return nil, QueueNotFoundError{ResourcePath: resourcePath}
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	now := time.Now()

	// Peek at the top job
	job := state.ReadyHeap.Peek()
	if job == nil {
		return nil, nil // No jobs available
	}

	// Check if job is visible
	if job.VisibleAt.After(now) {
		return nil, nil // Not yet visible
	}

	// Pop the job
	popped := state.ReadyHeap.PopJob()
	if popped == nil {
		return nil, nil // No jobs available
	}
	return popped, nil
}

// InitializeQueue initializes a queue by loading state from disk
func (m *Manager) InitializeQueue(resourcePath string) error {
	// Validate queue exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return QueueNotFoundError{ResourcePath: resourcePath}
		}
		return err
	}

	// Partition 0 for MVP
	partition := int32(0)

	// List segments
	segments, err := m.logManager.ListSegments(resourcePath, partition)
	if err != nil {
		return fmt.Errorf("failed to list segments: %w", err)
	}

	// Get or create queue state
	state := m.getOrCreateQueueState(resourcePath)

	state.mu.Lock()
	defer state.mu.Unlock()

	if len(segments) == 0 {
		// New queue, initialize empty state
		state.NextSeq = 0
		return nil
	}

	// Recover sequence from last segment
	lastSeq, err := m.recoverLastSeq(resourcePath, partition)
	if err != nil {
		return fmt.Errorf("failed to recover last sequence: %w", err)
	}

	state.NextSeq = lastSeq + 1

	// Rebuild ReadyHeap by reading all messages
	if err := m.rebuildReadyHeap(resourcePath, partition, state); err != nil {
		return fmt.Errorf("failed to rebuild ReadyHeap: %w", err)
	}

	m.log.Info().
		Str("resource", resourcePath).
		Int64("next_seq", state.NextSeq).
		Int("ready_jobs", state.ReadyHeap.Len()).
		Int("segments", len(segments)).
		Msg("Queue initialized")

	return nil
}

// recoverLastSeq recovers the last sequence number by reading the last segment
func (m *Manager) recoverLastSeq(resourcePath string, partition int32) (int64, error) {
	segments, err := m.logManager.ListSegments(resourcePath, partition)
	if err != nil {
		return -1, err
	}

	if len(segments) == 0 {
		return -1, nil // No messages
	}

	// Read last segment to find last sequence
	lastSegment := segments[len(segments)-1]
	reader, err := log.NewSegmentReader(lastSegment.Path)
	if err != nil {
		return -1, err
	}
	defer reader.Close()

	var lastSeq int64 = -1
	for {
		data, _, err := reader.ReadEntry()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return -1, err
		}

		msg, err := log.DecodeMessage(data)
		if err != nil {
			continue // Skip corrupted messages
		}

		if msg.Partition == partition && msg.Type == log.MessageTypeQueue && msg.Seq > lastSeq {
			lastSeq = msg.Seq
		}
	}

	return lastSeq, nil
}

// rebuildReadyHeap rebuilds the ReadyHeap from log entries
func (m *Manager) rebuildReadyHeap(resourcePath string, partition int32, state *QueueState) error {
	segments, err := m.logManager.ListSegments(resourcePath, partition)
	if err != nil {
		return err
	}

	now := time.Now()

	// Read all segments and rebuild heap
	for _, seg := range segments {
		reader, err := log.NewSegmentReader(seg.Path)
		if err != nil {
			continue // Skip segments that can't be read
		}

		for {
			data, offset, readErr := reader.ReadEntry()
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					break
				}
				// Skip corrupted entries, continue reading
				continue
			}

			// Decode message
			msg, decodeErr := log.DecodeMessage(data)
			if decodeErr != nil {
				// Skip corrupted messages, continue reading
				continue
			}

			// Only process queue messages for this partition
			if msg.Partition != partition || msg.Type != log.MessageTypeQueue {
				continue
			}

			// Check if job is already completed (not in InFlight, so assume ready if visible)
			// For MVP, we assume all jobs in log are ready (InFlight is in-memory only)
			if msg.VisibleAt.Before(now) || msg.VisibleAt.Equal(now) {
				// Create job metadata
				jobMetadata := &JobMetadata{
					ID:           msg.ID,
					Seq:          msg.Seq,
					VisibleAt:    msg.VisibleAt,
					ReserveUntil: time.Time{},
					PayloadPos: log.FilePointer{
						File:   seg.Path,
						Offset: offset,
					},
					Attempts:  msg.Attempts,
					CreatedAt: msg.CreatedAt,
				}

				// Add to ReadyHeap
				state.ReadyHeap.PushJob(jobMetadata)
			}
		}

		reader.Close()
	}

	return nil
}
