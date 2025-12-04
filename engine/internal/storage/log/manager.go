package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	// DefaultMaxSegmentSize is the default maximum segment size (100MB)
	DefaultMaxSegmentSize = 100 * 1024 * 1024
	// DefaultMaxSegmentAge is the default maximum segment age (24 hours)
	DefaultMaxSegmentAge = 24 * time.Hour
)

// Manager manages log segments for resources
type Manager struct {
	mu             sync.RWMutex
	baseDir        string
	segments       map[string]*ActiveSegment
	maxSegmentSize int64
	maxSegmentAge  time.Duration
}

// ActiveSegment represents an actively open segment
type ActiveSegment struct {
	metadata  *SegmentMetadata
	writer    *SegmentWriter
	createdAt time.Time
	lastWrite time.Time
}

// NewManager creates a new log manager
func NewManager(baseDir string) *Manager {
	return &Manager{
		baseDir:        baseDir,
		segments:       make(map[string]*ActiveSegment),
		maxSegmentSize: DefaultMaxSegmentSize,
		maxSegmentAge:  DefaultMaxSegmentAge,
	}
}

// SetMaxSegmentSize sets the maximum segment size
func (m *Manager) SetMaxSegmentSize(size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxSegmentSize = size
}

// SetMaxSegmentAge sets the maximum segment age
func (m *Manager) SetMaxSegmentAge(age time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxSegmentAge = age
}

// OpenSegment opens or creates a segment for a resource and partition
func (m *Manager) OpenSegment(resourcePath string, partition int32) (*SegmentWriter, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := segmentKey(resourcePath, partition)

	// Check if segment is already open
	if active, exists := m.segments[key]; exists {
		// Check if we should rotate
		if m.shouldRotate(active) {
			if err := m.rotateSegmentLocked(key, resourcePath, partition); err != nil {
				return nil, err
			}
			// Get the newly created segment
			active = m.segments[key]
		}
		active.lastWrite = time.Now()
		return active.writer, nil
	}

	// Create new segment
	return m.createSegmentLocked(key, resourcePath, partition)
}

// createSegmentLocked creates a new segment (assumes lock is held)
func (m *Manager) createSegmentLocked(key, resourcePath string, partition int32) (*SegmentWriter, error) {
	// Get resource directory (simple hash-based approach)
	resourceDir := getResourceHashDir(m.baseDir, resourcePath)

	// List existing segments to determine next segment number
	segments, err := m.listSegmentsLocked(resourceDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list segments: %w", err)
	}

	segmentNum := 1
	if len(segments) > 0 {
		// Get the highest segment number and increment
		segmentNum = segments[len(segments)-1] + 1
	}

	// Create segment path
	segmentPath := getSegmentPath(resourceDir, segmentNum)

	// Create segment writer
	writer, err := NewSegmentWriter(segmentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment writer: %w", err)
	}

	// Create metadata
	metadata := &SegmentMetadata{
		Path:         segmentPath,
		StartOffset:  0,
		EndOffset:    -1,
		StartSeq:     0,
		EndSeq:       -1,
		Size:         0,
		CreatedAt:    time.Now(),
		MessageCount: 0,
	}

	// Store active segment
	active := &ActiveSegment{
		metadata:  metadata,
		writer:    writer,
		createdAt: time.Now(),
		lastWrite: time.Now(),
	}

	m.segments[key] = active

	log.Debug().
		Str("resource", resourcePath).
		Int32("partition", partition).
		Str("segment", segmentPath).
		Msg("Segment opened")

	return writer, nil
}

// rotateSegmentLocked rotates a segment (assumes lock is held)
func (m *Manager) rotateSegmentLocked(key, resourcePath string, partition int32) error {
	active, exists := m.segments[key]
	if !exists {
		return nil
	}

	// Close current segment
	if err := active.writer.Close(); err != nil {
		log.Error().Err(err).Str("segment", active.metadata.Path).Msg("Failed to close segment during rotation")
	}

	// Remove from active segments
	delete(m.segments, key)

	log.Info().
		Str("resource", resourcePath).
		Int32("partition", partition).
		Str("segment", active.metadata.Path).
		Msg("Segment rotated")

	// Create new segment
	_, err := m.createSegmentLocked(key, resourcePath, partition)
	return err
}

// shouldRotate checks if a segment should be rotated
func (m *Manager) shouldRotate(active *ActiveSegment) bool {
	// Check size
	if active.metadata.Size >= m.maxSegmentSize {
		return true
	}

	// Check age
	if time.Since(active.createdAt) >= m.maxSegmentAge {
		return true
	}

	return false
}

// CloseSegment closes a segment
func (m *Manager) CloseSegment(resourcePath string, partition int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := segmentKey(resourcePath, partition)
	active, exists := m.segments[key]
	if !exists {
		return nil
	}

	if err := active.writer.Close(); err != nil {
		return err
	}

	delete(m.segments, key)

	log.Debug().
		Str("resource", resourcePath).
		Int32("partition", partition).
		Msg("Segment closed")

	return nil
}

// ListSegments lists all segments for a resource and partition
func (m *Manager) ListSegments(resourcePath string, partition int32) ([]*SegmentMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	resourceDir := getResourceHashDir(m.baseDir, resourcePath)

	segmentNums, err := m.listSegmentsLocked(resourceDir)
	if err != nil {
		return nil, err
	}

	var segments []*SegmentMetadata
	for _, num := range segmentNums {
		segmentPath := getSegmentPath(resourceDir, num)
		metadata, err := m.readSegmentMetadata(segmentPath)
		if err != nil {
			log.Warn().Err(err).Str("segment", segmentPath).Msg("Failed to read segment metadata")
			continue
		}
		segments = append(segments, metadata)
	}

	return segments, nil
}

// listSegmentsLocked lists segment numbers (assumes lock is held)
func (m *Manager) listSegmentsLocked(resourceDir string) ([]int, error) {
	files, err := filepath.Glob(filepath.Join(resourceDir, "segment-*.log"))
	if err != nil {
		return nil, err
	}

	var segmentNums []int
	for _, file := range files {
		var num int
		_, err := fmt.Sscanf(filepath.Base(file), "segment-%d.log", &num)
		if err != nil {
			continue
		}
		segmentNums = append(segmentNums, num)
	}

	// Sort segment numbers (simple bubble sort for small arrays)
	for i := 0; i < len(segmentNums)-1; i++ {
		for j := i + 1; j < len(segmentNums); j++ {
			if segmentNums[i] > segmentNums[j] {
				segmentNums[i], segmentNums[j] = segmentNums[j], segmentNums[i]
			}
		}
	}

	return segmentNums, nil
}

// readSegmentMetadata reads metadata for a segment file
func (m *Manager) readSegmentMetadata(path string) (*SegmentMetadata, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	return &SegmentMetadata{
		Path:      path,
		Size:      stat.Size(),
		CreatedAt: stat.ModTime(),
	}, nil
}

// FlushAll flushes all open segments
func (m *Manager) FlushAll() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for key, active := range m.segments {
		if err := active.writer.Flush(); err != nil {
			log.Error().Err(err).Str("key", key).Msg("Failed to flush segment")
			return err
		}
	}

	return nil
}

// CloseAll closes all open segments
func (m *Manager) CloseAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for key, active := range m.segments {
		if err := active.writer.Close(); err != nil {
			log.Error().Err(err).Str("key", key).Msg("Failed to close segment")
			lastErr = err
		}
	}

	m.segments = make(map[string]*ActiveSegment)
	return lastErr
}

// segmentKey creates a unique key for a resource and partition
func segmentKey(resourcePath string, partition int32) string {
	return fmt.Sprintf("%s:%d", resourcePath, partition)
}

// getResourceHashDir returns the directory path for a resource based on its hash
func getResourceHashDir(baseDir string, resourcePath string) string {
	hash := hashResourcePath(resourcePath)
	return filepath.Join(baseDir, hash)
}

// hashResourcePath creates a simple hash from resource path
func hashResourcePath(path string) string {
	var hash uint32 = 2166136261
	for _, c := range path {
		hash ^= uint32(c)
		hash *= 16777619
	}
	hashStr := fmt.Sprintf("%08x", hash)
	if len(hashStr) > 8 {
		hashStr = hashStr[:8]
	}
	return hashStr
}

// getSegmentPath returns the full path for a segment file
func getSegmentPath(resourceDir string, segmentNum int) string {
	filename := fmt.Sprintf("segment-%06d.log", segmentNum)
	return filepath.Join(resourceDir, filename)
}
