package consumers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/metrics"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/rs/zerolog"
)

const (
	// DefaultConsumersFile is the default filename for consumer offsets persistence
	DefaultConsumersFile = "consumers.json"
)

// LatestOffsetFunc is a function type that retrieves the latest offset for a stream partition
type LatestOffsetFunc func(stream string, partition int32) (int64, error)

// Manager manages consumer group offsets and state
type Manager struct {
	metaStore        *metastore.Store
	filePath         string
	offsets          map[string]*ConsumerOffset // key: offsetKey(stream, group, partition)
	mu               sync.RWMutex
	log              zerolog.Logger
	latestOffsetFunc LatestOffsetFunc              // Function to get latest offset for lag calculation
	metrics          *metrics.ConsumerGroupMetrics // Optional metrics collector
	ready            bool
}

// NewManager creates a new consumer group manager
func NewManager(metaStore *metastore.Store, metadataDir string, latestOffsetFunc LatestOffsetFunc, consumerMetrics ...*metrics.ConsumerGroupMetrics) *Manager {
	filePath := filepath.Join(metadataDir, DefaultConsumersFile)
	var cm *metrics.ConsumerGroupMetrics
	if len(consumerMetrics) > 0 && consumerMetrics[0] != nil {
		cm = consumerMetrics[0]
	}
	return &Manager{
		metaStore:        metaStore,
		filePath:         filePath,
		offsets:          make(map[string]*ConsumerOffset),
		log:              logger.WithComponent("consumers"),
		latestOffsetFunc: latestOffsetFunc,
		metrics:          cm,
		ready:            false,
	}
}

// Start initializes and starts the manager
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ready {
		return nil
	}

	// Load existing offsets from disk
	if err := m.load(); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to load consumer offsets: %w", err)
		}
		m.log.Info().Str("file", m.filePath).Msg("Consumer offsets file does not exist, will be created on first commit")
	}

	m.ready = true
	m.log.Info().Msg("Consumer group manager started")
	return nil
}

// Stop gracefully stops the manager
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.ready {
		return nil
	}

	// Flush pending offsets to disk
	if err := m.flush(); err != nil {
		m.log.Error().Err(err).Msg("Failed to flush consumer offsets on stop")
		return fmt.Errorf("failed to flush consumer offsets: %w", err)
	}

	m.ready = false
	m.log.Info().Msg("Consumer group manager stopped")
	return nil
}

// Ready returns true if the manager is ready
func (m *Manager) Ready() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ready
}

// CommitOffset commits an offset for a consumer group
func (m *Manager) CommitOffset(ctx context.Context, stream, group string, partition int32, offset int64) error {
	// Validate stream exists
	_, err := m.metaStore.GetResource(stream)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return CommitOffsetError{Stream: stream, Group: group, Err: fmt.Errorf("stream not found: %s", stream)}
		}
		return CommitOffsetError{Stream: stream, Group: group, Err: err}
	}

	// Validate offset is non-negative
	if offset < -1 {
		return InvalidOffsetError{Stream: stream, Group: group, Offset: offset, Reason: "offset cannot be negative"}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := offsetKey(stream, group, partition)
	now := time.Now()

	offsetEntry := &ConsumerOffset{
		Stream:    stream,
		Group:     group,
		Partition: partition,
		Offset:    offset,
		UpdatedAt: now,
	}

	// Update or create offset entry
	m.offsets[key] = offsetEntry

	// Persist to disk
	if err := m.flush(); err != nil {
		// Remove from memory if flush failed
		delete(m.offsets, key)
		return CommitOffsetError{Stream: stream, Group: group, Err: fmt.Errorf("failed to persist offset: %w", err)}
	}

	m.log.Debug().
		Str("stream", stream).
		Str("group", group).
		Int32("partition", partition).
		Int64("offset", offset).
		Bool("sandbox", IsSandboxGroup(group)).
		Msg("Offset committed")

	// Record metrics
	if m.metrics != nil {
		// Extract tenant/namespace from stream path (format: tenant/namespace/stream/name)
		parts := strings.Split(stream, "/")
		if len(parts) >= 4 {
			tenant := parts[0]
			namespace := parts[1]
			streamName := parts[3]
			m.metrics.RecordOffsetCommit(tenant, namespace, streamName, group, partition)
			m.metrics.UpdateCommittedOffset(tenant, namespace, streamName, group, partition, offset)
		}
	}

	return nil
}

// GetCommittedOffset retrieves the committed offset for a consumer group
func (m *Manager) GetCommittedOffset(ctx context.Context, stream, group string, partition int32) (int64, error) {
	// Validate stream exists
	_, err := m.metaStore.GetResource(stream)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return -1, GetOffsetError{Stream: stream, Group: group, Err: fmt.Errorf("stream not found: %s", stream)}
		}
		return -1, GetOffsetError{Stream: stream, Group: group, Err: err}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	key := offsetKey(stream, group, partition)
	offsetEntry, exists := m.offsets[key]
	if !exists {
		// Return -1 if no commits yet (consumer hasn't committed)
		return -1, nil
	}

	return offsetEntry.Offset, nil
}

// GetConsumerGroupState retrieves the complete state of a consumer group
func (m *Manager) GetConsumerGroupState(ctx context.Context, stream, group string, partition int32) (*ConsumerGroupState, error) {
	// Get committed offset
	committedOffset, err := m.GetCommittedOffset(ctx, stream, group, partition)
	if err != nil {
		return nil, err
	}

	// Get latest offset
	var latestOffset int64 = -1
	if m.latestOffsetFunc != nil {
		latestOffset, err = m.latestOffsetFunc(stream, partition)
		if err != nil {
			return nil, CalculateLagError{Stream: stream, Group: group, Err: err}
		}
	}

	// Calculate lag
	var lag int64 = -1
	if latestOffset >= 0 {
		if committedOffset < 0 {
			// No commits yet, lag equals latest offset + 1 (we haven't read anything)
			lag = latestOffset + 1
		} else {
			lag = latestOffset - committedOffset
			// Lag should not be negative
			if lag < 0 {
				lag = 0
			}
		}
	}

	state := &ConsumerGroupState{
		Stream:          stream,
		Group:           group,
		Partition:       partition,
		CommittedOffset: committedOffset,
		LatestOffset:    latestOffset,
		Lag:             lag,
	}

	// Update metrics
	if m.metrics != nil && lag >= 0 {
		// Extract tenant/namespace from stream path (format: tenant/namespaces/namespace/streams/name)
		parts := strings.Split(stream, "/")
		if len(parts) >= 4 {
			tenant := parts[0]
			namespace := parts[1]
			streamName := parts[3]
			m.metrics.UpdateLag(tenant, namespace, streamName, group, partition, lag)
		}
	}

	return state, nil
}

// CalculateLag calculates the lag for a consumer group
func (m *Manager) CalculateLag(ctx context.Context, stream, group string, partition int32) (int64, error) {
	state, err := m.GetConsumerGroupState(ctx, stream, group, partition)
	if err != nil {
		return -1, err
	}

	return state.Lag, nil
}

// ListConsumerGroups returns all consumer groups for a stream
func (m *Manager) ListConsumerGroups(ctx context.Context, stream string) ([]string, error) {
	// Validate stream exists
	_, err := m.metaStore.GetResource(stream)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, fmt.Errorf("stream not found: %s", stream)
		}
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	groupsMap := make(map[string]bool)
	for _, offset := range m.offsets {
		if offset.Stream == stream {
			groupsMap[offset.Group] = true
		}
	}

	groups := make([]string, 0, len(groupsMap))
	for group := range groupsMap {
		groups = append(groups, group)
	}

	return groups, nil
}

// DeleteConsumerGroup deletes all offsets for a consumer group
func (m *Manager) DeleteConsumerGroup(ctx context.Context, stream, group string) error {
	// Validate stream exists
	_, err := m.metaStore.GetResource(stream)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return fmt.Errorf("stream not found: %s", stream)
		}
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Find and delete all offsets for this group on this stream
	var deleted bool
	for key, offset := range m.offsets {
		if offset.Stream == stream && offset.Group == group {
			delete(m.offsets, key)
			deleted = true
		}
	}

	if !deleted {
		return ConsumerGroupNotFoundError{Stream: stream, Group: group}
	}

	// Persist deletion to disk
	if err := m.flush(); err != nil {
		return fmt.Errorf("failed to persist deletion: %w", err)
	}

	m.log.Info().
		Str("stream", stream).
		Str("group", group).
		Msg("Consumer group deleted")

	return nil
}

// load loads consumer offsets from disk
func (m *Manager) load() error {
	data, err := os.ReadFile(m.filePath)
	if err != nil {
		return err
	}

	var offsets map[string]*ConsumerOffset
	if err := json.Unmarshal(data, &offsets); err != nil {
		return fmt.Errorf("failed to unmarshal consumer offsets: %w", err)
	}

	m.offsets = offsets

	m.log.Info().
		Str("file", m.filePath).
		Int("count", len(m.offsets)).
		Msg("Consumer offsets loaded from disk")

	return nil
}

// flush persists consumer offsets to disk
func (m *Manager) flush() error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(m.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(m.offsets, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal consumer offsets: %w", err)
	}

	// Write to temporary file first, then rename (atomic write)
	tmpFile := m.filePath + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0600); err != nil {
		return fmt.Errorf("failed to write consumer offsets file: %w", err)
	}

	if err := os.Rename(tmpFile, m.filePath); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("failed to rename consumer offsets file: %w", err)
	}

	return nil
}
