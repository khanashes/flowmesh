package streams

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/flowmesh/engine/internal/storage/log"
)

const (
	// DefaultIndexInterval is the default interval for sparse indexing (every 1000 messages)
	DefaultIndexInterval = 1000
)

// IndexEntry represents a single entry in the sparse offset index
type IndexEntry struct {
	Offset      int64  `json:"offset"`
	SegmentPath string `json:"segment_path"`
	FileOffset  int64  `json:"file_offset"`
}

// OffsetIndex maintains a sparse index mapping offsets to segment positions
type OffsetIndex struct {
	Entries       []IndexEntry `json:"entries"`
	IndexInterval int64        `json:"index_interval"`
}

// NewOffsetIndex creates a new offset index with default interval
func NewOffsetIndex() *OffsetIndex {
	return &OffsetIndex{
		Entries:       make([]IndexEntry, 0),
		IndexInterval: DefaultIndexInterval,
	}
}

// AddEntry adds an index entry for the given offset
func (idx *OffsetIndex) AddEntry(offset int64, segmentPath string, fileOffset int64) {
	entry := IndexEntry{
		Offset:      offset,
		SegmentPath: segmentPath,
		FileOffset:  fileOffset,
	}

	// Insert in sorted order (by offset)
	idx.Entries = append(idx.Entries, entry)
	sort.Slice(idx.Entries, func(i, j int) bool {
		return idx.Entries[i].Offset < idx.Entries[j].Offset
	})
}

// FindSegment finds the segment and approximate file offset for a given message offset
// Returns the segment path and file offset, or error if not found
func (idx *OffsetIndex) FindSegment(offset int64) (string, int64, error) {
	if len(idx.Entries) == 0 {
		return "", 0, fmt.Errorf("index is empty")
	}

	// Binary search for the entry with the largest offset <= target offset
	left := 0
	right := len(idx.Entries) - 1
	var bestEntry *IndexEntry

	for left <= right {
		mid := (left + right) / 2
		entry := &idx.Entries[mid]

		if entry.Offset <= offset {
			bestEntry = entry
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if bestEntry == nil {
		// Target offset is before first entry, use first entry
		bestEntry = &idx.Entries[0]
	}

	return bestEntry.SegmentPath, bestEntry.FileOffset, nil
}

// Rebuild rebuilds the index from segment metadata
// This is called during recovery to rebuild the index from existing segments
func (idx *OffsetIndex) Rebuild(segments []*log.SegmentMetadata) error {
	idx.Entries = make([]IndexEntry, 0)

	// Sort segments by start offset
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].StartOffset < segments[j].StartOffset
	})

	// For each segment, add an entry at the start offset
	for _, seg := range segments {
		if seg.StartOffset >= 0 {
			idx.AddEntry(seg.StartOffset, seg.Path, 0)
		}
	}

	return nil
}

// Save persists the index to disk as JSON
func (idx *OffsetIndex) Save(path string) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create index directory: %w", err)
	}

	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal index: %w", err)
	}

	// Write to temp file first, then rename (atomic write)
	tmpFile := path + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0o600); err != nil {
		return fmt.Errorf("failed to write index file: %w", err)
	}

	if err := os.Rename(tmpFile, path); err != nil {
		//nolint:errcheck // Ignore remove error
		_ = os.Remove(tmpFile)
		return fmt.Errorf("failed to rename index file: %w", err)
	}

	return nil
}

// Load loads the index from disk
func (idx *OffsetIndex) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Index doesn't exist yet, that's okay
			return nil
		}
		return fmt.Errorf("failed to read index file: %w", err)
	}

	if err := json.Unmarshal(data, idx); err != nil {
		return fmt.Errorf("failed to unmarshal index: %w", err)
	}

	// Ensure entries are sorted
	sort.Slice(idx.Entries, func(i, j int) bool {
		return idx.Entries[i].Offset < idx.Entries[j].Offset
	})

	return nil
}
