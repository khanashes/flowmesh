package test_test

import (
	"path/filepath"
	"testing"

	"github.com/flowmesh/engine/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExample demonstrates basic test utilities usage
func TestExample(t *testing.T) {
	// Create a temporary directory
	dir := test.TempDir(t)
	assert.DirExists(t, dir)

	// Create a test data directory structure
	dataDir := test.CreateTestDataDir(t, dir)
	assert.DirExists(t, filepath.Join(dataDir, "streams"))
	assert.DirExists(t, filepath.Join(dataDir, "queues"))
	assert.DirExists(t, filepath.Join(dataDir, "kv"))
	assert.DirExists(t, filepath.Join(dataDir, "metadata"))

	// Create a temporary file
	file := test.TempFile(t, dir, "test-*.log")
	test.AssertFileExists(t, file)

	// Verify the file path is correct
	require.True(t, filepath.IsAbs(file))
}
