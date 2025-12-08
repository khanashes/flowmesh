package streams

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOffsetIndex_AddEntry(t *testing.T) {
	idx := NewOffsetIndex()

	idx.AddEntry(0, "/path/to/segment-1.log", 0)
	idx.AddEntry(1000, "/path/to/segment-1.log", 1024)
	idx.AddEntry(500, "/path/to/segment-1.log", 512)

	// Should be sorted by offset
	assert.Equal(t, int64(0), idx.Entries[0].Offset)
	assert.Equal(t, int64(500), idx.Entries[1].Offset)
	assert.Equal(t, int64(1000), idx.Entries[2].Offset)
}

func TestOffsetIndex_FindSegment(t *testing.T) {
	idx := NewOffsetIndex()

	idx.AddEntry(0, "/path/to/segment-1.log", 0)
	idx.AddEntry(1000, "/path/to/segment-2.log", 0)
	idx.AddEntry(2000, "/path/to/segment-3.log", 0)

	// Find exact match
	path, _, err := idx.FindSegment(1000)
	require.NoError(t, err)
	assert.Equal(t, "/path/to/segment-2.log", path)

	// Find closest lower match
	path, _, err = idx.FindSegment(1500)
	require.NoError(t, err)
	assert.Equal(t, "/path/to/segment-2.log", path)

	// Find before first entry
	path, _, err = idx.FindSegment(500)
	require.NoError(t, err)
	assert.Equal(t, "/path/to/segment-1.log", path)
}

func TestOffsetIndex_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "index.json")

	idx1 := NewOffsetIndex()
	idx1.AddEntry(0, "/path/to/segment-1.log", 0)
	idx1.AddEntry(1000, "/path/to/segment-2.log", 1024)

	err := idx1.Save(indexPath)
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(indexPath)
	require.NoError(t, err)

	// Load index
	idx2 := NewOffsetIndex()
	err = idx2.Load(indexPath)
	require.NoError(t, err)

	assert.Equal(t, len(idx1.Entries), len(idx2.Entries))
	assert.Equal(t, idx1.Entries[0].Offset, idx2.Entries[0].Offset)
	assert.Equal(t, idx1.Entries[1].Offset, idx2.Entries[1].Offset)
}

func TestOffsetIndex_LoadNonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "nonexistent.json")

	idx := NewOffsetIndex()
	err := idx.Load(indexPath)
	require.NoError(t, err) // Should not error if file doesn't exist
	assert.Equal(t, 0, len(idx.Entries))
}
