package streams

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimestampIndex_AddEntry(t *testing.T) {
	idx := NewTimestampIndex()

	t1 := time.Unix(1000, 0)
	t2 := time.Unix(2000, 0)
	t3 := time.Unix(1500, 0)

	idx.AddEntry(t1, 0, "/path/to/segment-1.log", 0)
	idx.AddEntry(t2, 1000, "/path/to/segment-2.log", 1024)
	idx.AddEntry(t3, 500, "/path/to/segment-1.log", 512)

	// Should be sorted by timestamp
	assert.Equal(t, int64(1000000000000), idx.Entries[0].Timestamp)
	assert.Equal(t, int64(1500000000000), idx.Entries[1].Timestamp)
	assert.Equal(t, int64(2000000000000), idx.Entries[2].Timestamp)
}

func TestTimestampIndex_ShouldIndex(t *testing.T) {
	idx := NewTimestampIndex()
	idx.IndexInterval = 1000
	idx.TimeInterval = int64(time.Minute)

	t.Run("should index by message count", func(t *testing.T) {
		idx.LastIndexedOffset = -1
		assert.True(t, idx.ShouldIndex(0, time.Now()))

		// After indexing, both should be set
		idx.LastIndexedOffset = 0
		idx.LastIndexedTime = time.Now().UnixNano() // Set time to prevent time-based indexing
		assert.False(t, idx.ShouldIndex(500, time.Now()))
		assert.True(t, idx.ShouldIndex(1000, time.Now()))
	})

	t.Run("should index by time interval", func(t *testing.T) {
		idx.LastIndexedTime = 0
		idx.LastIndexedOffset = 0
		assert.True(t, idx.ShouldIndex(500, time.Now()))

		now := time.Now()
		idx.LastIndexedTime = now.UnixNano()
		idx.LastIndexedOffset = 500
		assert.False(t, idx.ShouldIndex(600, now.Add(30*time.Second)))
		assert.True(t, idx.ShouldIndex(600, now.Add(2*time.Minute)))
	})
}

func TestTimestampIndex_FindOffsetByTimestamp(t *testing.T) {
	idx := NewTimestampIndex()

	t1 := time.Unix(1000, 0)
	t2 := time.Unix(2000, 0)
	t3 := time.Unix(3000, 0)

	idx.AddEntry(t1, 0, "/path/to/segment-1.log", 0)
	idx.AddEntry(t2, 1000, "/path/to/segment-2.log", 0)
	idx.AddEntry(t3, 2000, "/path/to/segment-3.log", 0)

	// Find exact match
	offset, path, _, err := idx.FindOffsetByTimestamp(t2)
	require.NoError(t, err)
	assert.Equal(t, int64(1000), offset)
	assert.Equal(t, "/path/to/segment-2.log", path)

	// Find closest lower match
	offset, path, _, err = idx.FindOffsetByTimestamp(time.Unix(2500, 0))
	require.NoError(t, err)
	assert.Equal(t, int64(1000), offset)
	assert.Equal(t, "/path/to/segment-2.log", path)

	// Find before first entry
	offset, path, _, err = idx.FindOffsetByTimestamp(time.Unix(500, 0))
	require.NoError(t, err)
	assert.Equal(t, int64(0), offset)
	assert.Equal(t, "/path/to/segment-1.log", path)
}

func TestTimestampIndex_FindOffsetByTimestamp_Empty(t *testing.T) {
	idx := NewTimestampIndex()

	_, _, _, err := idx.FindOffsetByTimestamp(time.Now())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timestamp index is empty")
}

func TestTimestampIndex_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "timestamp_index.json")

	idx1 := NewTimestampIndex()
	t1 := time.Unix(1000, 0)
	t2 := time.Unix(2000, 0)
	idx1.AddEntry(t1, 0, "/path/to/segment-1.log", 0)
	idx1.AddEntry(t2, 1000, "/path/to/segment-2.log", 1024)

	err := idx1.Save(indexPath)
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(indexPath)
	require.NoError(t, err)

	// Load index
	idx2 := NewTimestampIndex()
	err = idx2.Load(indexPath)
	require.NoError(t, err)

	assert.Equal(t, len(idx1.Entries), len(idx2.Entries))
	assert.Equal(t, idx1.Entries[0].Timestamp, idx2.Entries[0].Timestamp)
	assert.Equal(t, idx1.Entries[1].Timestamp, idx2.Entries[1].Timestamp)
	assert.Equal(t, idx1.Entries[0].Offset, idx2.Entries[0].Offset)
	assert.Equal(t, idx1.Entries[1].Offset, idx2.Entries[1].Offset)
}

func TestTimestampIndex_LoadNonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "nonexistent.json")

	idx := NewTimestampIndex()
	err := idx.Load(indexPath)
	require.NoError(t, err) // Should not error if file doesn't exist
	assert.Equal(t, 0, len(idx.Entries))
}

func TestTimestampIndex_Rebuild(t *testing.T) {
	idx := NewTimestampIndex()

	segments := []*log.SegmentMetadata{
		{
			Path:        "/path/to/segment-1.log",
			StartOffset: 0,
			CreatedAt:   time.Unix(1000, 0),
		},
		{
			Path:        "/path/to/segment-2.log",
			StartOffset: 1000,
			CreatedAt:   time.Unix(2000, 0),
		},
		{
			Path:        "/path/to/segment-3.log",
			StartOffset: 2000,
			CreatedAt:   time.Unix(3000, 0),
		},
	}

	err := idx.Rebuild(segments)
	require.NoError(t, err)

	assert.Equal(t, 3, len(idx.Entries))
	assert.Equal(t, int64(0), idx.Entries[0].Offset)
	assert.Equal(t, int64(1000), idx.Entries[1].Offset)
	assert.Equal(t, int64(2000), idx.Entries[2].Offset)
}

func TestTimestampIndex_Rebuild_IgnoresInvalidSegments(t *testing.T) {
	idx := NewTimestampIndex()

	segments := []*log.SegmentMetadata{
		{
			Path:        "/path/to/segment-1.log",
			StartOffset: -1, // Invalid
			CreatedAt:   time.Unix(1000, 0),
		},
		{
			Path:        "/path/to/segment-2.log",
			StartOffset: 0,
			CreatedAt:   time.Unix(2000, 0),
		},
	}

	err := idx.Rebuild(segments)
	require.NoError(t, err)

	// Should only have one entry (the valid segment)
	assert.Equal(t, 1, len(idx.Entries))
	assert.Equal(t, int64(0), idx.Entries[0].Offset)
}
