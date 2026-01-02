package test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TempDir creates a temporary directory for testing and returns its path.
// The directory is automatically cleaned up after the test.
func TempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "flowmesh-test-*")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(dir) // Ignore cleanup errors in tests
	})
	return dir
}

// TempFile creates a temporary file for testing and returns its path.
// The file is automatically cleaned up after the test.
func TempFile(t *testing.T, dir, pattern string) string {
	t.Helper()
	if dir == "" {
		dir = TempDir(t)
	}
	file, err := os.CreateTemp(dir, pattern)
	require.NoError(t, err)
	_ = file.Close() // Ignore close error
	t.Cleanup(func() {
		_ = os.Remove(file.Name()) // Ignore cleanup errors in tests
	})
	return file.Name()
}

// CleanupDir removes a directory and all its contents.
func CleanupDir(dir string) error {
	return os.RemoveAll(dir)
}

// CreateTestDataDir creates a test data directory structure.
func CreateTestDataDir(t *testing.T, baseDir string) string {
	t.Helper()
	dataDir := filepath.Join(baseDir, "data")
	//nolint:gosec // Acceptable: test directory permissions
	err := os.MkdirAll(filepath.Join(dataDir, "streams"), 0755)
	require.NoError(t, err)
	//nolint:gosec // Acceptable: test directory permissions
	err = os.MkdirAll(filepath.Join(dataDir, "queues"), 0755)
	require.NoError(t, err)
	//nolint:gosec // Acceptable: test directory permissions
	err = os.MkdirAll(filepath.Join(dataDir, "kv"), 0755)
	require.NoError(t, err)
	err = os.MkdirAll(filepath.Join(dataDir, "metadata"), 0755)
	require.NoError(t, err)
	return dataDir
}

// AssertFileExists checks if a file exists and fails the test if it doesn't.
func AssertFileExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.NoError(t, err, "file should exist: %s", path)
}

// AssertFileNotExists checks if a file doesn't exist and fails the test if it does.
func AssertFileNotExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.Error(t, err, "file should not exist: %s", path)
	require.True(t, os.IsNotExist(err), "expected file not to exist: %s", path)
}
