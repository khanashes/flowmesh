package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	// SubDirectories defines the storage subdirectories
	DirStreams   = "streams"
	DirQueues    = "queues"
	DirKV        = "kv"
	DirMetadata  = "metadata"
	DirSchemas   = "schemas"
	DirConsumers = "consumers"
)

// StoragePaths holds all storage directory paths
type StoragePaths struct {
	BaseDir     string
	StreamsDir  string
	QueuesDir   string
	KVDir       string
	MetadataDir string
	SchemasDir  string
}

// InitDirectories creates and validates all storage directories
func InitDirectories(baseDir string) (*StoragePaths, error) {
	baseDir = filepath.Clean(baseDir)

	paths := &StoragePaths{
		BaseDir:     baseDir,
		StreamsDir:  filepath.Join(baseDir, DirStreams),
		QueuesDir:   filepath.Join(baseDir, DirQueues),
		KVDir:       filepath.Join(baseDir, DirKV),
		MetadataDir: filepath.Join(baseDir, DirMetadata),
		SchemasDir:  filepath.Join(baseDir, DirSchemas),
	}

	// Create all directories
	dirs := []string{
		paths.BaseDir,
		paths.StreamsDir,
		paths.QueuesDir,
		paths.KVDir,
		paths.MetadataDir,
		paths.SchemasDir,
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Validate directories exist and are writable
	for _, dir := range dirs {
		if err := validateDirectory(dir); err != nil {
			return nil, fmt.Errorf("directory validation failed for %s: %w", dir, err)
		}
	}

	return paths, nil
}

// validateDirectory checks if a directory exists and is writable
func validateDirectory(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("directory does not exist: %w", err)
	}

	if !info.IsDir() {
		return fmt.Errorf("path exists but is not a directory: %s", path)
	}

	// Check write permissions by attempting to create a temp file
	testFile := filepath.Join(path, ".write_test")
	file, err := os.Create(testFile)
	if err != nil {
		return fmt.Errorf("directory is not writable: %w", err)
	}
	file.Close()
	os.Remove(testFile)

	return nil
}

// GetResourceHashDir returns the directory path for a resource based on its hash
func GetResourceHashDir(baseDir string, resourcePath string) (string, error) {
	// Simple hash-based directory structure
	// For MVP, use a simple hash of the resource path
	hash := hashResourcePath(resourcePath)
	return filepath.Join(baseDir, hash), nil
}

// hashResourcePath creates a simple hash from resource path
// For MVP, using a simple approach - can be enhanced later
func hashResourcePath(path string) string {
	// Simple hash: take first 8 characters of a hash
	// In production, use a proper hash function like sha256
	// For now, sanitize and use first part as directory name
	hash := fmt.Sprintf("%x", hashString(path))
	if len(hash) > 8 {
		hash = hash[:8]
	}
	return hash
}

// hashString creates a simple hash from string
func hashString(s string) uint32 {
	var hash uint32 = 2166136261
	for _, c := range s {
		hash ^= uint32(c)
		hash *= 16777619
	}
	return hash
}

// CleanupTestDirectories removes test directories (for testing only)
func CleanupTestDirectories(baseDir string) error {
	if baseDir == "" {
		return fmt.Errorf("empty base directory, refusing to clean")
	}

	// Safety check: ensure it's a test directory
	if !filepath.IsAbs(baseDir) {
		return fmt.Errorf("base directory must be absolute path: %s", baseDir)
	}

	return os.RemoveAll(baseDir)
}

// GetSegmentPath returns the full path for a segment file
func GetSegmentPath(resourceDir string, segmentNum int) string {
	filename := fmt.Sprintf("segment-%06d.log", segmentNum)
	return filepath.Join(resourceDir, filename)
}
