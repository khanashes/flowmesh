package log

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSegmentWriter_WriteEntry(t *testing.T) {
	tmpDir := t.TempDir()
	segmentPath := filepath.Join(tmpDir, "segment-000001.log")

	writer, err := NewSegmentWriter(segmentPath, FsyncAlways)
	require.NoError(t, err)
	defer writer.Close()

	// Write test data
	testData := []byte("hello, world!")
	err = writer.WriteEntry(testData)
	require.NoError(t, err)

	// Verify file was created and has content
	stat, err := os.Stat(segmentPath)
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))

	// Flush to ensure data is written
	err = writer.Flush()
	require.NoError(t, err)
}

func TestSegmentReader_ReadEntry(t *testing.T) {
	tmpDir := t.TempDir()
	segmentPath := filepath.Join(tmpDir, "segment-000001.log")

	// Write some entries
	writer, err := NewSegmentWriter(segmentPath, FsyncAlways)
	require.NoError(t, err)

	testData1 := []byte("first entry")
	testData2 := []byte("second entry")

	require.NoError(t, writer.WriteEntry(testData1))
	require.NoError(t, writer.WriteEntry(testData2))
	require.NoError(t, writer.Flush())
	writer.Close()

	// Read back entries
	reader, err := NewSegmentReader(segmentPath)
	require.NoError(t, err)
	defer reader.Close()

	// Read first entry
	data1, offset1, err := reader.ReadEntry()
	require.NoError(t, err)
	assert.Equal(t, testData1, data1)
	assert.Equal(t, int64(0), offset1)

	// Read second entry
	data2, offset2, err := reader.ReadEntry()
	require.NoError(t, err)
	assert.Equal(t, testData2, data2)
	assert.Greater(t, offset2, offset1)

	// Should get EOF on next read
	_, _, err = reader.ReadEntry()
	assert.Error(t, err)
}

func TestSegmentWriter_ChecksumValidation(t *testing.T) {
	tmpDir := t.TempDir()
	segmentPath := filepath.Join(tmpDir, "segment-000001.log")

	// Write entry
	writer, err := NewSegmentWriter(segmentPath, FsyncAlways)
	require.NoError(t, err)

	testData := []byte("test data")
	require.NoError(t, writer.WriteEntry(testData))
	writer.Close()

	// Corrupt the file by modifying a byte
	file, err := os.OpenFile(segmentPath, os.O_RDWR, 0644)
	require.NoError(t, err)
	file.Seek(10, 0)        // Skip header
	file.Write([]byte{'X'}) // Corrupt data
	file.Close()

	// Try to read - should detect corruption
	reader, err := NewSegmentReader(segmentPath)
	require.NoError(t, err)
	defer reader.Close()

	_, _, err = reader.ReadEntry()
	assert.Error(t, err)
	assert.IsType(t, ChecksumMismatchError{}, err)
}

func TestSegmentWriter_MultipleEntries(t *testing.T) {
	tmpDir := t.TempDir()
	segmentPath := filepath.Join(tmpDir, "segment-000001.log")

	writer, err := NewSegmentWriter(segmentPath, FsyncAlways)
	require.NoError(t, err)

	// Write multiple entries
	entries := [][]byte{
		[]byte("entry 1"),
		[]byte("entry 2"),
		[]byte("entry 3"),
		[]byte("entry 4"),
	}

	for _, entry := range entries {
		require.NoError(t, writer.WriteEntry(entry))
	}

	require.NoError(t, writer.Flush())
	writer.Close()

	// Read all entries back
	reader, err := NewSegmentReader(segmentPath)
	require.NoError(t, err)
	defer reader.Close()

	for i, expected := range entries {
		data, _, err := reader.ReadEntry()
		require.NoError(t, err, "Failed to read entry %d", i)
		assert.Equal(t, expected, data, "Entry %d mismatch", i)
	}

	// Should be EOF now
	_, _, err = reader.ReadEntry()
	assert.Error(t, err)
}

func TestValidateSegment(t *testing.T) {
	tmpDir := t.TempDir()
	segmentPath := filepath.Join(tmpDir, "segment-000001.log")

	// Create valid segment
	writer, err := NewSegmentWriter(segmentPath, FsyncAlways)
	require.NoError(t, err)

	require.NoError(t, writer.WriteEntry([]byte("test")))
	writer.Close()

	// Should validate successfully
	err = ValidateSegment(segmentPath)
	assert.NoError(t, err)

	// Non-existent file should fail
	err = ValidateSegment(filepath.Join(tmpDir, "nonexistent.log"))
	assert.Error(t, err)
}
