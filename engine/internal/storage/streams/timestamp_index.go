package streams

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/flowmesh/engine/internal/storage/log"
)

const (
	// DefaultTimestampIndexInterval is the default interval for timestamp indexing (every 1000 messages)
	DefaultTimestampIndexInterval = 1000
	// DefaultTimestampTimeInterval is the default time interval for timestamp indexing (every minute)
	DefaultTimestampTimeInterval = time.Minute
)

// TimestampIndexEntry represents a single entry in the sparse timestamp index
type TimestampIndexEntry struct {
	Timestamp   int64  `json:"timestamp"`    // Unix nanoseconds
	Offset      int64  `json:"offset"`       // Message offset
	SegmentPath string `json:"segment_path"` // Path to segment file
	FileOffset  int64  `json:"file_offset"`  // Byte offset within segment
}

// TimestampIndex maintains a sparse index mapping timestamps to offsets
type TimestampIndex struct {
	Entries           []TimestampIndexEntry `json:"entries"`
	IndexInterval     int64                 `json:"index_interval"`      // Message count interval
	TimeInterval      int64                 `json:"time_interval"`       // Nanoseconds interval
	LastIndexedTime   int64                 `json:"last_indexed_time"`   // Last timestamp indexed
	LastIndexedOffset int64                 `json:"last_indexed_offset"` // Last offset indexed
}

// NewTimestampIndex creates a new timestamp index with default intervals
func NewTimestampIndex() *TimestampIndex {
	return &TimestampIndex{
		Entries:           make([]TimestampIndexEntry, 0),
		IndexInterval:     DefaultTimestampIndexInterval,
		TimeInterval:      int64(DefaultTimestampTimeInterval),
		LastIndexedTime:   0,
		LastIndexedOffset: -1,
	}
}

// ShouldIndex checks if we should add an index entry for the given offset and timestamp
func (idx *TimestampIndex) ShouldIndex(offset int64, timestamp time.Time) bool {
	tsNanos := timestamp.UnixNano()

	// Check message count interval condition
	messageCountExceeded := idx.LastIndexedOffset < 0 || (offset-idx.LastIndexedOffset) >= idx.IndexInterval
	if messageCountExceeded {
		return true
	}

	// Check time interval condition
	// If we've never indexed at all (both are uninitialized), we should index
	if idx.LastIndexedOffset < 0 && idx.LastIndexedTime == 0 {
		return true
	}

	// If LastIndexedTime == 0, we've never indexed by time
	// Index if LastIndexedOffset <= 0 (never indexed or initial state)
	// Don't index if LastIndexedOffset > 0 (we've indexed by offset but not by time - wait for message count)
	if idx.LastIndexedTime == 0 {
		return idx.LastIndexedOffset <= 0
	}

	// We've indexed by time before, check if time interval exceeded
	if (tsNanos - idx.LastIndexedTime) >= idx.TimeInterval {
		return true
	}

	return false
}

// AddEntry adds a timestamp index entry
func (idx *TimestampIndex) AddEntry(timestamp time.Time, offset int64, segmentPath string, fileOffset int64) {
	tsNanos := timestamp.UnixNano()
	entry := TimestampIndexEntry{
		Timestamp:   tsNanos,
		Offset:      offset,
		SegmentPath: segmentPath,
		FileOffset:  fileOffset,
	}

	// Insert in sorted order (by timestamp)
	idx.Entries = append(idx.Entries, entry)
	sort.Slice(idx.Entries, func(i, j int) bool {
		return idx.Entries[i].Timestamp < idx.Entries[j].Timestamp
	})

	idx.LastIndexedTime = tsNanos
	idx.LastIndexedOffset = offset
}

// FindOffsetByTimestamp finds the approximate offset for a given timestamp
// Returns the offset and segment info, or error if not found
func (idx *TimestampIndex) FindOffsetByTimestamp(timestamp time.Time) (int64, string, int64, error) {
	if len(idx.Entries) == 0 {
		return 0, "", 0, fmt.Errorf("timestamp index is empty")
	}

	targetNanos := timestamp.UnixNano()

	// Binary search for the entry with the largest timestamp <= target timestamp
	left := 0
	right := len(idx.Entries) - 1
	var bestEntry *TimestampIndexEntry

	for left <= right {
		mid := (left + right) / 2
		entry := &idx.Entries[mid]

		if entry.Timestamp <= targetNanos {
			bestEntry = entry
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if bestEntry == nil {
		// Target timestamp is before first entry, use first entry
		bestEntry = &idx.Entries[0]
	}

	return bestEntry.Offset, bestEntry.SegmentPath, bestEntry.FileOffset, nil
}

// Rebuild rebuilds the timestamp index from segment metadata
// This is a simplified rebuild that adds entries at segment boundaries
// A full rebuild would require reading messages from segments, but for now
// we use segment creation time as an approximation
func (idx *TimestampIndex) Rebuild(segments []*log.SegmentMetadata) error {
	idx.Entries = make([]TimestampIndexEntry, 0)
	idx.LastIndexedTime = 0
	idx.LastIndexedOffset = -1

	// Sort segments by start offset
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].StartOffset < segments[j].StartOffset
	})

	// For each segment, add an entry at the start
	// This is a simplified rebuild - a full rebuild would scan actual messages
	for _, seg := range segments {
		if seg.StartOffset < 0 {
			continue
		}

		// Use segment creation time as timestamp approximation
		if seg.CreatedAt.UnixNano() > 0 {
			idx.AddEntry(seg.CreatedAt, seg.StartOffset, seg.Path, 0)
		}
	}

	return nil
}

// Save persists the timestamp index to disk as JSON
func (idx *TimestampIndex) Save(path string) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create index directory: %w", err)
	}

	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal timestamp index: %w", err)
	}

	// Write to temp file first, then rename (atomic write)
	tmpFile := path + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0o600); err != nil {
		return fmt.Errorf("failed to write timestamp index file: %w", err)
	}

	if err := os.Rename(tmpFile, path); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("failed to rename timestamp index file: %w", err)
	}

	return nil
}

// Load loads the timestamp index from disk
func (idx *TimestampIndex) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Index doesn't exist yet, that's okay
			return nil
		}
		return fmt.Errorf("failed to read timestamp index file: %w", err)
	}

	if err := json.Unmarshal(data, idx); err != nil {
		return fmt.Errorf("failed to unmarshal timestamp index: %w", err)
	}

	// Ensure entries are sorted
	sort.Slice(idx.Entries, func(i, j int) bool {
		return idx.Entries[i].Timestamp < idx.Entries[j].Timestamp
	})

	return nil
}
