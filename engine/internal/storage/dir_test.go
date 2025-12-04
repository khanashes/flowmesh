package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitDirectories(t *testing.T) {
	tmpDir := t.TempDir()

	paths, err := InitDirectories(tmpDir)
	require.NoError(t, err)
	assert.NotNil(t, paths)

	// Verify all directories were created
	assert.DirExists(t, paths.BaseDir)
	assert.DirExists(t, paths.StreamsDir)
	assert.DirExists(t, paths.QueuesDir)
	assert.DirExists(t, paths.KVDir)
	assert.DirExists(t, paths.MetadataDir)
	assert.DirExists(t, paths.SchemasDir)
}

func TestGetSegmentPath(t *testing.T) {
	resourceDir := "/test/resource"
	path := GetSegmentPath(resourceDir, 1)
	expected := filepath.Join(resourceDir, "segment-000001.log")
	assert.Equal(t, expected, path)

	path = GetSegmentPath(resourceDir, 123)
	expected = filepath.Join(resourceDir, "segment-000123.log")
	assert.Equal(t, expected, path)
}

func TestCleanupTestDirectories(t *testing.T) {
	tmpDir := t.TempDir()

	// Create some files
	err := os.WriteFile(filepath.Join(tmpDir, "test.txt"), []byte("test"), 0644)
	require.NoError(t, err)

	// Should fail with relative path
	err = CleanupTestDirectories("relative/path")
	assert.Error(t, err)

	// Should succeed with absolute path
	err = CleanupTestDirectories(tmpDir)
	assert.NoError(t, err)

	// Directory should be gone
	_, err = os.Stat(tmpDir)
	assert.True(t, os.IsNotExist(err))
}
