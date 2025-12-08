package streams

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/rs/zerolog"
)

// Event represents an event to be written to a stream
type Event struct {
	Payload []byte
	Headers map[string]string
}

// StreamState represents the state of a stream
type StreamState struct {
	ResourcePath string
	Partitions   map[int32]int64 // partition -> next offset
	mu           sync.RWMutex
}

// Manager manages stream operations
type Manager struct {
	metaStore    *metastore.Store
	logManager   *log.Manager
	metadataDir  string
	streamStates map[string]*StreamState
	indexes      map[string]*OffsetIndex
	log          zerolog.Logger
	mu           sync.RWMutex
}

// NewManager creates a new stream manager
func NewManager(metaStore *metastore.Store, logManager *log.Manager, metadataDir string) *Manager {
	return &Manager{
		metaStore:    metaStore,
		logManager:   logManager,
		metadataDir:  metadataDir,
		streamStates: make(map[string]*StreamState),
		indexes:      make(map[string]*OffsetIndex),
		log:          logger.WithComponent("streams"),
	}
}

// getOrCreateStreamState gets or creates stream state for a resource
func (m *Manager) getOrCreateStreamState(resourcePath string) *StreamState {
	m.mu.Lock()
	defer m.mu.Unlock()

	if state, exists := m.streamStates[resourcePath]; exists {
		return state
	}

	state := &StreamState{
		ResourcePath: resourcePath,
		Partitions:   make(map[int32]int64),
	}
	m.streamStates[resourcePath] = state
	return state
}

// getOrCreateIndex gets or creates offset index for a resource
func (m *Manager) getOrCreateIndex(resourcePath string) *OffsetIndex {
	m.mu.Lock()
	defer m.mu.Unlock()

	if idx, exists := m.indexes[resourcePath]; exists {
		return idx
	}

	idx := NewOffsetIndex()
	m.indexes[resourcePath] = idx
	return idx
}

// WriteEvents writes events to a stream and returns assigned offsets
func (m *Manager) WriteEvents(resourcePath string, events []Event) ([]int64, error) {
	// Validate stream exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, StreamNotFoundError{ResourcePath: resourcePath}
		}
		return nil, WriteError{ResourcePath: resourcePath, Err: err}
	}

	// Get or create stream state
	state := m.getOrCreateStreamState(resourcePath)
	idx := m.getOrCreateIndex(resourcePath)

	// Partition 0 for MVP
	partition := int32(0)

	state.mu.Lock()
	defer state.mu.Unlock()

	// Get next offset for partition
	nextOffset, exists := state.Partitions[partition]
	if !exists {
		nextOffset = 0
	}

	// Get active segment writer
	writer, err := m.logManager.OpenSegment(resourcePath, partition)
	if err != nil {
		return nil, WriteError{ResourcePath: resourcePath, Err: err}
	}

	offsets := make([]int64, 0, len(events))

	for i, event := range events {
		// Assign offset
		offset := nextOffset + int64(i)

		// Generate unique ID
		idBytes := make([]byte, 16)
		if _, err := rand.Read(idBytes); err != nil {
			return nil, WriteError{ResourcePath: resourcePath, Err: fmt.Errorf("failed to generate ID: %w", err)}
		}

		// Create message
		msg := &log.Message{
			ID:            hex.EncodeToString(idBytes),
			ResourcePath:  resourcePath,
			Partition:     partition,
			Offset:        offset,
			Seq:           0, // Not used for streams
			Type:          log.MessageTypeStream,
			Payload:       event.Payload,
			Headers:       event.Headers,
			CreatedAt:     time.Now(),
			VisibleAt:     time.Time{}, // Not used for streams
			Attempts:      0,           // Not used for streams
			SchemaVersion: 0,           // Future: schema versioning
		}

		// Encode message
		encoded, err := log.EncodeMessage(msg)
		if err != nil {
			return nil, WriteError{ResourcePath: resourcePath, Err: fmt.Errorf("failed to encode message: %w", err)}
		}

		// Write to segment
		if err := writer.WriteEntry(encoded); err != nil {
			return nil, WriteError{ResourcePath: resourcePath, Err: fmt.Errorf("failed to write entry: %w", err)}
		}

		// Update sparse index (every Nth message)
		currentFileOffset := writer.Offset()
		if offset%idx.IndexInterval == 0 {
			// Get current segment path
			segments, err := m.logManager.ListSegments(resourcePath, partition)
			if err == nil && len(segments) > 0 {
				// Use the last segment (most recent)
				lastSegment := segments[len(segments)-1]
				idx.AddEntry(offset, lastSegment.Path, currentFileOffset)
			}
		}

		offsets = append(offsets, offset)
	}

	// Update offset counter
	state.Partitions[partition] = nextOffset + int64(len(events))

	// Flush segment
	if err := writer.Flush(); err != nil {
		m.log.Warn().Err(err).Str("resource", resourcePath).Msg("Failed to flush segment after write")
	}

	m.log.Debug().
		Str("resource", resourcePath).
		Int("count", len(events)).
		Int64("start_offset", offsets[0]).
		Int64("end_offset", offsets[len(offsets)-1]).
		Msg("Events written to stream")

	return offsets, nil
}

// ReadFromOffset reads messages from a stream starting at the given offset
func (m *Manager) ReadFromOffset(resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error) {
	// Validate stream exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, StreamNotFoundError{ResourcePath: resourcePath}
		}
		return nil, ReadError{ResourcePath: resourcePath, Offset: offset, Err: err}
	}

	// Validate offset
	if offset < 0 {
		return nil, InvalidOffsetError{
			Offset:       offset,
			ResourcePath: resourcePath,
			Reason:       "offset cannot be negative",
		}
	}

	// Read messages
	messages := make([]*log.Message, 0, maxMessages)

	// Read from segments until we have enough messages or reach the end
	segments, err := m.logManager.ListSegments(resourcePath, partition)
	if err != nil {
		return nil, ReadError{ResourcePath: resourcePath, Offset: offset, Err: err}
	}

	if len(segments) == 0 {
		return []*log.Message{}, nil // No messages yet
	}

	// Read from all segments starting from the first
	for segIdx := 0; segIdx < len(segments) && len(messages) < maxMessages; segIdx++ {
		seg := segments[segIdx]
		reader, err := log.NewSegmentReader(seg.Path)
		if err != nil {
			return nil, ReadError{ResourcePath: resourcePath, Offset: offset, Err: err}
		}

		// Read entries from this segment
		for len(messages) < maxMessages {
			data, _, readErr := reader.ReadEntry()
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					break // End of segment
				}
				reader.Close()
				return nil, ReadError{ResourcePath: resourcePath, Offset: offset, Err: readErr}
			}

			// Decode message
			msg, decodeErr := log.DecodeMessage(data)
			if decodeErr != nil {
				reader.Close()
				return nil, ReadError{ResourcePath: resourcePath, Offset: offset, Err: fmt.Errorf("failed to decode message: %w", decodeErr)}
			}

			// Filter by partition and offset
			if msg.Partition != partition {
				continue
			}

			if msg.Offset < offset {
				continue // Skip messages before target offset
			}

			// Message offset >= target offset, include it
			messages = append(messages, msg)

			// If we've read enough, stop
			if len(messages) >= maxMessages {
				break
			}
		}

		reader.Close()
	}

	return messages, nil
}

// GetLatestOffset returns the latest offset for a partition
func (m *Manager) GetLatestOffset(resourcePath string, partition int32) (int64, error) {
	// Validate stream exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return -1, StreamNotFoundError{ResourcePath: resourcePath}
		}
		return -1, err
	}

	// Get stream state
	m.mu.RLock()
	state, exists := m.streamStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		// Try to recover from segments
		return m.recoverLatestOffset(resourcePath, partition)
	}

	state.mu.RLock()
	defer state.mu.RUnlock()

	offset, exists := state.Partitions[partition]
	if !exists {
		return -1, nil // No messages yet
	}

	return offset - 1, nil // Last written offset
}

// recoverLatestOffset recovers the latest offset by reading the last segment
func (m *Manager) recoverLatestOffset(resourcePath string, partition int32) (int64, error) {
	segments, err := m.logManager.ListSegments(resourcePath, partition)
	if err != nil {
		return -1, err
	}

	if len(segments) == 0 {
		return -1, nil // No messages
	}

	// Read last segment to find last offset
	lastSegment := segments[len(segments)-1]
	reader, err := log.NewSegmentReader(lastSegment.Path)
	if err != nil {
		return -1, err
	}
	defer reader.Close()

	var lastOffset int64 = -1
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

		if msg.Partition == partition && msg.Offset > lastOffset {
			lastOffset = msg.Offset
		}
	}

	return lastOffset, nil
}

// InitializeStream initializes a stream by loading state from disk
func (m *Manager) InitializeStream(resourcePath string) error {
	// Validate stream exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return StreamNotFoundError{ResourcePath: resourcePath}
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

	if len(segments) == 0 {
		// New stream, initialize empty state
		state := m.getOrCreateStreamState(resourcePath)
		state.mu.Lock()
		state.Partitions[partition] = 0
		state.mu.Unlock()
		return nil
	}

	// Recover offset from last segment
	lastOffset, err := m.recoverLatestOffset(resourcePath, partition)
	if err != nil {
		return fmt.Errorf("failed to recover latest offset: %w", err)
	}

	// Initialize stream state
	state := m.getOrCreateStreamState(resourcePath)
	state.mu.Lock()
	if lastOffset >= 0 {
		state.Partitions[partition] = lastOffset + 1
	} else {
		state.Partitions[partition] = 0
	}
	state.mu.Unlock()

	// Rebuild index
	idx := m.getOrCreateIndex(resourcePath)
	if err := idx.Rebuild(segments); err != nil {
		return fmt.Errorf("failed to rebuild index: %w", err)
	}

	// Load persisted index if it exists
	indexPath := m.getIndexPath(resourcePath)
	if err := idx.Load(indexPath); err != nil {
		m.log.Warn().Err(err).Str("resource", resourcePath).Msg("Failed to load index, will rebuild")
	}

	m.log.Info().
		Str("resource", resourcePath).
		Int64("latest_offset", lastOffset).
		Int("segments", len(segments)).
		Msg("Stream initialized")

	return nil
}

// getIndexPath returns the path to the index file for a resource
func (m *Manager) getIndexPath(resourcePath string) string {
	// Store index in metadata directory
	hash := hashResourcePath(resourcePath)
	indexDir := filepath.Join(m.metadataDir, "streams", hash)
	return filepath.Join(indexDir, "index.json")
}

// hashResourcePath creates a simple hash from resource path
func hashResourcePath(path string) string {
	var hash uint32 = 2166136261
	for _, c := range path {
		hash ^= uint32(c)
		hash *= 16777619
	}
	return fmt.Sprintf("%08x", hash)
}

// SaveIndexes saves all indexes to disk
func (m *Manager) SaveIndexes() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for resourcePath, idx := range m.indexes {
		indexPath := m.getIndexPath(resourcePath)
		if err := idx.Save(indexPath); err != nil {
			m.log.Error().Err(err).Str("resource", resourcePath).Msg("Failed to save index")
			return err
		}
	}

	return nil
}
