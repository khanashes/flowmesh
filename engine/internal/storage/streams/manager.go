package streams

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/flowmesh/engine/internal/filter"
	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/metrics"
	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/flowmesh/engine/internal/storage/schema"
	"github.com/flowmesh/engine/internal/tracing"
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
	metaStore        *metastore.Store
	logManager       *log.Manager
	metadataDir      string
	schemaRegistry   *schema.Registry
	validator        *schema.Validator
	streamStates     map[string]*StreamState
	indexes          map[string]*OffsetIndex
	timestampIndexes map[string]*TimestampIndex
	metrics          *metrics.StreamMetrics // Optional metrics collector
	log              zerolog.Logger
	mu               sync.RWMutex
}

// NewManager creates a new stream manager
func NewManager(metaStore *metastore.Store, logManager *log.Manager, metadataDir string, streamMetrics ...*metrics.StreamMetrics) *Manager {
	var sm *metrics.StreamMetrics
	if len(streamMetrics) > 0 && streamMetrics[0] != nil {
		sm = streamMetrics[0]
	}
	return &Manager{
		metaStore:        metaStore,
		logManager:       logManager,
		metadataDir:      metadataDir,
		schemaRegistry:   schema.NewRegistry(metaStore),
		validator:        schema.NewValidator(),
		streamStates:     make(map[string]*StreamState),
		indexes:          make(map[string]*OffsetIndex),
		timestampIndexes: make(map[string]*TimestampIndex),
		metrics:          sm,
		log:              logger.WithComponent("streams"),
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

// getOrCreateTimestampIndex gets or creates timestamp index for a resource
func (m *Manager) getOrCreateTimestampIndex(resourcePath string) *TimestampIndex {
	m.mu.Lock()
	defer m.mu.Unlock()

	if idx, exists := m.timestampIndexes[resourcePath]; exists {
		return idx
	}

	idx := NewTimestampIndex()
	m.timestampIndexes[resourcePath] = idx
	return idx
}

// WriteEvents writes events to a stream and returns assigned offsets
func (m *Manager) WriteEvents(ctx context.Context, resourcePath string, events []Event) ([]int64, error) {
	startTime := time.Now()

	// Partition 0 for MVP
	partition := int32(0)

	// Start tracing span
	ctx, span := StartWriteSpan(ctx, resourcePath, partition, len(events))
	defer span.End()

	// Validate stream exists and get config
	config, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, StreamNotFoundError{ResourcePath: resourcePath}
		}
		return nil, WriteError{ResourcePath: resourcePath, Err: err}
	}

	// Validate events against schema if schema is configured
	if config.Schema != nil {
		// Get schema (use latest if version is 0)
		var schemaDef *schema.Schema
		if config.Schema.Version > 0 {
			schemaDef, err = m.schemaRegistry.GetSchema(ctx, config.Tenant, config.Schema.ID, config.Schema.Version)
		} else {
			schemaDef, err = m.schemaRegistry.GetLatestSchema(ctx, config.Tenant, config.Schema.ID)
		}
		if err != nil {
			span.RecordError(err)
			return nil, WriteError{ResourcePath: resourcePath, Err: fmt.Errorf("failed to get schema: %w", err)}
		}

		// Validate each event payload
		for i, event := range events {
			if err := m.validator.Validate(event.Payload, schemaDef.Definition); err != nil {
				span.RecordError(err)
				return nil, WriteError{
					ResourcePath: resourcePath,
					Err:          fmt.Errorf("event %d validation failed: %w", i, err),
				}
			}
		}
	}

	// Get or create stream state
	state := m.getOrCreateStreamState(resourcePath)
	idx := m.getOrCreateIndex(resourcePath)
	timestampIdx := m.getOrCreateTimestampIndex(resourcePath)

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

	// Determine schema version once before the loop
	schemaVersion := int32(0)
	if config.Schema != nil {
		if config.Schema.Version > 0 {
			schemaVersion = config.Schema.Version
		} else {
			// Get latest version
			latestSchema, err := m.schemaRegistry.GetLatestSchema(ctx, config.Tenant, config.Schema.ID)
			if err == nil {
				schemaVersion = latestSchema.Version
			}
		}
	}

	for i, event := range events {
		// Assign offset
		offset := nextOffset + int64(i)

		// Generate unique ID
		idBytes := make([]byte, 16)
		if _, err := rand.Read(idBytes); err != nil {
			span.RecordError(err)
			return nil, WriteError{ResourcePath: resourcePath, Err: fmt.Errorf("failed to generate ID: %w", err)}
		}

		// Ensure headers map exists
		if event.Headers == nil {
			event.Headers = make(map[string]string)
		}

		// Inject trace context into event headers
		tracing.InjectToHeaders(ctx, event.Headers)

		// Create message with current timestamp
		createdAt := time.Now()
		msg := &log.Message{
			ID:            hex.EncodeToString(idBytes),
			ResourcePath:  resourcePath,
			Partition:     partition,
			Offset:        offset,
			Seq:           0, // Not used for streams
			Type:          log.MessageTypeStream,
			Payload:       event.Payload,
			Headers:       event.Headers,
			CreatedAt:     createdAt,
			VisibleAt:     time.Time{}, // Not used for streams
			Attempts:      0,           // Not used for streams
			SchemaVersion: schemaVersion,
		}

		// Encode message
		encoded, err := log.EncodeMessage(msg)
		if err != nil {
			span.RecordError(err)
			return nil, WriteError{ResourcePath: resourcePath, Err: fmt.Errorf("failed to encode message: %w", err)}
		}

		// Write to segment
		if err := writer.WriteEntry(encoded); err != nil {
			span.RecordError(err)
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

		// Update timestamp index (at intervals)
		if timestampIdx.ShouldIndex(offset, createdAt) {
			segments, err := m.logManager.ListSegments(resourcePath, partition)
			if err == nil && len(segments) > 0 {
				lastSegment := segments[len(segments)-1]
				timestampIdx.AddEntry(createdAt, offset, lastSegment.Path, currentFileOffset)
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

	// Record metrics
	if m.metrics != nil && len(offsets) > 0 {
		duration := time.Since(startTime)
		// Calculate total bytes written
		var totalBytes int64
		for _, event := range events {
			totalBytes += int64(len(event.Payload))
		}
		partition := int32(0)
		m.metrics.RecordWrite(config.Tenant, config.Namespace, config.Name, partition, len(events), totalBytes, duration)
		// Update offset gauge
		m.metrics.UpdateOffset(config.Tenant, config.Namespace, config.Name, partition, offsets[len(offsets)-1])
	}

	return offsets, nil
}

// ReadOptions options for reading from a stream
type ReadOptions struct {
	Offset      int64
	MaxMessages int
	Filter      string
}

// ReadFromOffset reads messages from a stream starting at the given offset
func (m *Manager) ReadFromOffset(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error) {
	return m.ReadWithOptions(ctx, resourcePath, partition, ReadOptions{
		Offset:      offset,
		MaxMessages: maxMessages,
	})
}

// ReadWithOptions reads messages with advanced options
func (m *Manager) ReadWithOptions(ctx context.Context, resourcePath string, partition int32, opts ReadOptions) ([]*log.Message, error) {
	startTime := time.Now()
	offset := opts.Offset
	maxMessages := opts.MaxMessages

	// Start tracing span
	_, span := StartReadSpan(ctx, resourcePath, partition, offset, maxMessages)
	defer span.End()

	// Parse filter if present
	var filterExpr filter.Expression
	if opts.Filter != "" {
		var err error
		filterExpr, err = filter.Parse(opts.Filter)
		if err != nil {
			return nil, fmt.Errorf("invalid filter expression: %w", err)
		}
	}

	// Validate stream exists
	config, err := m.metaStore.GetResource(resourcePath)
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
				_ = reader.Close() // Ignore close error
				return nil, ReadError{ResourcePath: resourcePath, Offset: offset, Err: readErr}
			}

			// Decode message
			msg, decodeErr := log.DecodeMessage(data)
			if decodeErr != nil {
				_ = reader.Close() // Ignore close error
				return nil, ReadError{ResourcePath: resourcePath, Offset: offset, Err: fmt.Errorf("failed to decode message: %w", decodeErr)}
			}

			// Filter by partition and offset
			if msg.Partition != partition {
				continue
			}

			if msg.Offset < offset {
				continue // Skip messages before target offset
			}

			// Apply filter if configured
			if filterExpr != nil {
				matchCtx := filter.Context{
					"headers":   msg.Headers,
					"partition": msg.Partition,
					"offset":    msg.Offset,
					"timestamp": msg.CreatedAt.Unix(),
				}
				result, err := filterExpr.Evaluate(matchCtx)
				if err != nil {
					// Log error but skip message? Or return error?
					// For now, treat evaluation error as no match to be safe
					m.log.Debug().Err(err).Msg("Filter evaluation failed")
					continue
				}
				if b, ok := result.(bool); !ok || !b {
					continue
				}
			}

			// Message offset >= target offset and matches filter, include it
			messages = append(messages, msg)

			// If we've read enough, stop
			if len(messages) >= maxMessages {
				break
			}
		}

		_ = reader.Close() // Ignore close error
	}

	// Record metrics
	if m.metrics != nil {
		duration := time.Since(startTime)
		// Calculate total bytes read
		var totalBytes int64
		for _, msg := range messages {
			totalBytes += int64(len(msg.Payload))
		}
		m.metrics.RecordRead(config.Tenant, config.Namespace, config.Name, partition, len(messages), totalBytes, duration)
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

// FindOffsetByTimestamp finds the approximate offset for a given timestamp
// Uses timestamp index to find approximate offset, then scans forward to find exact position
func (m *Manager) FindOffsetByTimestamp(ctx context.Context, resourcePath string, partition int32, timestamp time.Time) (int64, error) {
	// Validate stream exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return -1, StreamNotFoundError{ResourcePath: resourcePath}
		}
		return -1, err
	}

	// Get timestamp index
	timestampIdx := m.getOrCreateTimestampIndex(resourcePath)
	if len(timestampIdx.Entries) == 0 {
		return -1, fmt.Errorf("timestamp index is empty for stream %s", resourcePath)
	}

	// Find approximate offset from timestamp index
	approxOffset, _, _, err := timestampIdx.FindOffsetByTimestamp(timestamp)
	if err != nil {
		return -1, fmt.Errorf("failed to find offset by timestamp: %w", err)
	}

	// Scan forward from approximate offset to find exact position
	// Read messages starting from approximate offset
	messages, err := m.ReadFromOffset(ctx, resourcePath, partition, approxOffset, 1000)
	if err != nil {
		return -1, fmt.Errorf("failed to read messages for timestamp lookup: %w", err)
	}

	// Find the first message with timestamp >= target timestamp
	targetNanos := timestamp.UnixNano()
	for _, msg := range messages {
		if msg.CreatedAt.UnixNano() >= targetNanos {
			return msg.Offset, nil
		}
	}

	// If we didn't find it in the first batch, the timestamp might be beyond available messages
	// Return the last offset we checked
	if len(messages) > 0 {
		return messages[len(messages)-1].Offset, nil
	}

	return approxOffset, nil
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

	// Initialize timestamp index
	timestampIdx := m.getOrCreateTimestampIndex(resourcePath)
	timestampIndexPath := m.getTimestampIndexPath(resourcePath)
	if err := timestampIdx.Load(timestampIndexPath); err != nil {
		m.log.Warn().Err(err).Str("resource", resourcePath).Msg("Failed to load timestamp index, will rebuild on next write")
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

// getTimestampIndexPath returns the path to the timestamp index file for a resource
func (m *Manager) getTimestampIndexPath(resourcePath string) string {
	// Store timestamp index in metadata directory
	hash := hashResourcePath(resourcePath)
	indexDir := filepath.Join(m.metadataDir, "streams", hash)
	return filepath.Join(indexDir, "timestamp_index.json")
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

	// Save timestamp indexes
	for resourcePath, timestampIdx := range m.timestampIndexes {
		timestampIndexPath := m.getTimestampIndexPath(resourcePath)
		if err := timestampIdx.Save(timestampIndexPath); err != nil {
			m.log.Error().Err(err).Str("resource", resourcePath).Msg("Failed to save timestamp index")
			return err
		}
	}

	return nil
}

// Recover recovers the state of all streams from the log
func (m *Manager) Recover(ctx context.Context) error {
	m.log.Info().Msg("Starting stream recovery")

	// List all stream resources
	streams, err := m.metaStore.ListResources("", "", metastore.ResourceStream)
	if err != nil {
		return fmt.Errorf("failed to list streams: %w", err)
	}

	m.log.Info().Int("count", len(streams)).Msg("Found streams to recover")

	for _, config := range streams {
		path := config.GetPath()
		if err := m.InitializeStream(path); err != nil {
			m.log.Error().Err(err).Str("stream", path).Msg("Failed to recover stream")
			return fmt.Errorf("failed to recover stream %s: %w", path, err)
		}
	}

	m.log.Info().Msg("Stream recovery completed")
	return nil
}
