package metastore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	// DefaultMetadataFile is the default filename for metadata persistence
	DefaultMetadataFile = "resources.json"
)

// Store manages resource metadata with in-memory cache and disk persistence
type Store struct {
	mu        sync.RWMutex
	resources map[string]*ResourceConfig
	filePath  string
	dirty     bool
}

// NewStore creates a new metadata store
func NewStore(metadataDir string) (*Store, error) {
	filePath := filepath.Join(metadataDir, DefaultMetadataFile)

	store := &Store{
		resources: make(map[string]*ResourceConfig),
		filePath:  filePath,
		dirty:     false,
	}

	// Load existing metadata from disk
	if err := store.Load(); err != nil {
		// If file doesn't exist, that's okay - we'll create it on first write
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load metadata: %w", err)
		}
		log.Info().Str("file", filePath).Msg("Metadata file does not exist, will be created on first resource creation")
	}

	return store, nil
}

// CreateResource creates a new resource configuration
func (s *Store) CreateResource(config *ResourceConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := config.GetPath()

	// Check if resource already exists
	if _, exists := s.resources[path]; exists {
		return ResourceExistsError{Path: path}
	}

	// Set timestamps
	now := time.Now()
	if config.CreatedAt.IsZero() {
		config.CreatedAt = now
	}
	config.UpdatedAt = now

	// Store in memory
	s.resources[path] = config
	s.dirty = true

	// Flush to disk
	if err := s.flush(); err != nil {
		// Remove from memory if flush failed
		delete(s.resources, path)
		return fmt.Errorf("failed to persist resource: %w", err)
	}

	log.Info().
		Str("path", path).
		Str("type", string(config.Type)).
		Msg("Resource created")

	return nil
}

// GetResource retrieves a resource configuration by path
func (s *Store) GetResource(path string) (*ResourceConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	config, exists := s.resources[path]
	if !exists {
		return nil, ResourceNotFoundError{Path: path}
	}

	// Return a copy to prevent external modification
	return s.copyResource(config), nil
}

// ListResources lists resources matching the filters
func (s *Store) ListResources(tenant, namespace string, resourceType ResourceType) ([]*ResourceConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make([]*ResourceConfig, 0, len(s.resources))

	for path, config := range s.resources {
		// Filter by tenant
		if tenant != "" && config.Tenant != tenant {
			continue
		}

		// Filter by namespace
		if namespace != "" && config.Namespace != namespace {
			continue
		}

		// Filter by type
		if resourceType != "" && config.Type != resourceType {
			continue
		}

		// Only include if path matches (additional safety check)
		if config.GetPath() != path {
			continue
		}

		results = append(results, s.copyResource(config))
	}

	return results, nil
}

// UpdateResource updates an existing resource configuration
func (s *Store) UpdateResource(path string, updater func(*ResourceConfig) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	config, exists := s.resources[path]
	if !exists {
		return ResourceNotFoundError{Path: path}
	}

	// Create a copy for updating
	updated := s.copyResource(config)

	// Apply updates
	if err := updater(updated); err != nil {
		return fmt.Errorf("update failed: %w", err)
	}

	// Validate updated config
	if err := updated.Validate(); err != nil {
		return err
	}

	// Update timestamp
	updated.UpdatedAt = time.Now()

	// Store updated config
	s.resources[path] = updated
	s.dirty = true

	// Flush to disk
	if err := s.flush(); err != nil {
		// Revert if flush failed
		s.resources[path] = config
		return fmt.Errorf("failed to persist update: %w", err)
	}

	log.Info().Str("path", path).Msg("Resource updated")

	return nil
}

// DeleteResource removes a resource configuration
func (s *Store) DeleteResource(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.resources[path]; !exists {
		return ResourceNotFoundError{Path: path}
	}

	delete(s.resources, path)
	s.dirty = true

	// Flush to disk
	if err := s.flush(); err != nil {
		// Note: We can't easily revert deletion, but log the error
		log.Error().Err(err).Str("path", path).Msg("Failed to persist resource deletion")
		return fmt.Errorf("failed to persist deletion: %w", err)
	}

	log.Info().Str("path", path).Msg("Resource deleted")

	return nil
}

// Load loads metadata from disk
func (s *Store) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return err
	}

	var resources map[string]*ResourceConfig
	if err := json.Unmarshal(data, &resources); err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	s.resources = resources
	s.dirty = false

	log.Info().
		Str("file", s.filePath).
		Int("count", len(s.resources)).
		Msg("Metadata loaded from disk")

	return nil
}

// Flush persists metadata to disk
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.flush()
}

// flush persists to disk without locking (assumes lock is held)
func (s *Store) flush() error {
	if !s.dirty {
		return nil
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(s.resources, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Write to temporary file first, then rename (atomic write)
	tmpFile := s.filePath + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0o600); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}

	if err := os.Rename(tmpFile, s.filePath); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("failed to rename metadata file: %w", err)
	}

	s.dirty = false

	return nil
}

// copyResource creates a deep copy of a resource config
func (s *Store) copyResource(config *ResourceConfig) *ResourceConfig {
	copied := *config

	// Deep copy pointers
	if config.DLQ != nil {
		dlqCopy := *config.DLQ
		copied.DLQ = &dlqCopy
	}

	if config.Schema != nil {
		schemaCopy := *config.Schema
		if len(config.Schema.Definition) > 0 {
			schemaCopy.Definition = make([]byte, len(config.Schema.Definition))
			copy(schemaCopy.Definition, config.Schema.Definition)
		}
		copied.Schema = &schemaCopy
	}

	// Deep copy headers if present (for future use)
	// Note: Headers are in Message, not ResourceConfig, so this is placeholder

	return &copied
}
