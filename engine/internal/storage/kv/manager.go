package kv

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json" // Added for Scan method
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings" // Added for Scan method
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/flowmesh/engine/internal/filter" // Added for Scan method
	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage/metastore" // Added for Scan method
	"github.com/rs/zerolog"
)

// Manager manages KV store operations
type Manager struct {
	metaStore    *metastore.Store
	kvBaseDir    string
	dbs          map[string]*pebble.DB       // resourcePath -> DB
	expiryIndex  map[time.Time][]expiryEntry // expiresAt -> []{resourcePath, key}
	log          zerolog.Logger
	mu           sync.RWMutex
	ready        bool
	reaperStopCh chan struct{}
}

type expiryEntry struct {
	resourcePath string
	key          string
}

// NewManager creates a new KV manager
func NewManager(metaStore *metastore.Store, kvBaseDir string) *Manager {
	return &Manager{
		metaStore:    metaStore,
		kvBaseDir:    kvBaseDir,
		dbs:          make(map[string]*pebble.DB),
		expiryIndex:  make(map[time.Time][]expiryEntry),
		log:          logger.WithComponent("kv"),
		reaperStopCh: make(chan struct{}),
	}
}

// Start initializes and starts the KV manager
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ready {
		return nil
	}

	m.log.Info().Msg("Starting KV manager...")

	// Start TTL reaper
	m.reaperStopCh = make(chan struct{})
	go m.runTTLReaper(ctx, m.reaperStopCh)

	m.ready = true
	m.log.Info().Msg("KV manager started")

	return nil
}

// Stop gracefully stops the KV manager
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.ready {
		return nil
	}

	m.log.Info().Msg("Stopping KV manager...")

	// Stop TTL reaper
	close(m.reaperStopCh)

	// Close all databases
	var lastErr error
	for resourcePath, db := range m.dbs {
		if err := db.Close(); err != nil {
			m.log.Error().Err(err).Str("resource_path", resourcePath).Msg("Failed to close Pebble DB")
			lastErr = err
		}
	}
	m.dbs = make(map[string]*pebble.DB)
	m.expiryIndex = make(map[time.Time][]expiryEntry)

	m.ready = false
	m.log.Info().Msg("KV manager stopped")

	return lastErr
}

// Ready returns true if the KV manager is ready
func (m *Manager) Ready() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ready
}

// getOrOpenDB gets or opens a Pebble DB for a resource
func (m *Manager) getOrOpenDB(resourcePath string) (*pebble.DB, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if db, exists := m.dbs[resourcePath]; exists {
		return db, nil
	}

	// Get resource hash directory (using same hashing approach as streams/queues)
	hash := hashResourcePath(resourcePath)
	resourceDir := filepath.Join(m.kvBaseDir, hash)

	// Create db subdirectory
	dbDir := filepath.Join(resourceDir, "db")
	if err := ensureDirectory(dbDir); err != nil {
		return nil, fmt.Errorf("failed to create DB directory: %w", err)
	}

	// Open Pebble DB
	opts := &pebble.Options{}
	db, err := pebble.Open(dbDir, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open Pebble DB: %w", err)
	}

	m.dbs[resourcePath] = db
	return db, nil
}

// encodeKey encodes userKey (each KV store has its own DB, so no need for resourcePath)
func encodeKey(userKey string) []byte {
	return []byte(userKey)
}

// decodeKey decodes a key (just returns the userKey since resourcePath is in DB selection)
func decodeKey(encodedKey []byte) string {
	return string(encodedKey)
}

// encodeValue encodes a Value struct to bytes using GOB
func encodeValue(v *Value) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("failed to encode value: %w", err)
	}
	return buf.Bytes(), nil
}

// decodeValue decodes bytes to a Value struct using GOB
func decodeValue(data []byte) (*Value, error) {
	var v Value
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("failed to decode value: %w", err)
	}
	return &v, nil
}

// Set sets a key-value pair in the KV store
func (m *Manager) Set(ctx context.Context, resourcePath, key string, value []byte, options SetOptions) error {
	// Start tracing span
	_, span := StartSetSpan(ctx, resourcePath, key)
	defer span.End()

	if key == "" {
		return InvalidKeyError{Key: key, Reason: "key cannot be empty"}
	}

	// Validate KV store exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return KVStoreNotFoundError{ResourcePath: resourcePath}
		}
		return fmt.Errorf("failed to validate KV store: %w", err)
	}

	// Get or open DB
	db, err := m.getOrOpenDB(resourcePath)
	if err != nil {
		return fmt.Errorf("failed to get DB: %w", err)
	}

	// Create value with metadata
	now := time.Now()
	val := &Value{
		Payload:   value,
		CreatedAt: now,
	}

	// Set expiration if TTL is specified
	if options.TTL > 0 {
		expiresAt := now.Add(options.TTL)
		val.ExpiresAt = &expiresAt

		// Add to expiry index
		m.addToExpiryIndex(expiresAt, resourcePath, key)
	}

	// Encode value
	encodedValue, err := encodeValue(val)
	if err != nil {
		return fmt.Errorf("failed to encode value: %w", err)
	}

	// Encode key
	encodedKey := encodeKey(key)

	// Write to Pebble
	wo := &pebble.WriteOptions{}
	if err := db.Set(encodedKey, encodedValue, wo); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to set key: %w", err)
	}

	return nil
}

// Get retrieves a value by key from the KV store
func (m *Manager) Get(ctx context.Context, resourcePath, key string) ([]byte, error) {
	// Start tracing span
	ctx, span := StartGetSpan(ctx, resourcePath, key)
	defer span.End()

	if key == "" {
		return nil, InvalidKeyError{Key: key, Reason: "key cannot be empty"}
	}

	// Validate KV store exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, KVStoreNotFoundError{ResourcePath: resourcePath}
		}
		return nil, fmt.Errorf("failed to validate KV store: %w", err)
	}

	// Get or open DB
	db, err := m.getOrOpenDB(resourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get DB: %w", err)
	}

	// Encode key
	encodedKey := encodeKey(key)

	// Read from Pebble
	valueBytes, closer, err := db.Get(encodedKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, KeyNotFoundError{ResourcePath: resourcePath, Key: key}
		}
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get key: %w", err)
	}
	defer closer.Close()

	// Copy value bytes (closer will free the original)
	valueCopy := make([]byte, len(valueBytes))
	copy(valueCopy, valueBytes)

	// Decode value
	val, err := decodeValue(valueCopy)
	if err != nil {
		return nil, fmt.Errorf("failed to decode value: %w", err)
	}

	// Check expiration
	if val.ExpiresAt != nil && time.Now().After(*val.ExpiresAt) {
		// Key has expired, delete it
		m.Delete(ctx, resourcePath, key)
		return nil, KeyExpiredError{ResourcePath: resourcePath, Key: key}
	}

	return val.Payload, nil
}

// Delete deletes a key from the KV store
func (m *Manager) Delete(ctx context.Context, resourcePath, key string) error {
	// Start tracing span
	_, span := StartDeleteSpan(ctx, resourcePath, key)
	defer span.End()

	if key == "" {
		return InvalidKeyError{Key: key, Reason: "key cannot be empty"}
	}

	// Validate KV store exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return KVStoreNotFoundError{ResourcePath: resourcePath}
		}
		return fmt.Errorf("failed to validate KV store: %w", err)
	}

	// Get or open DB
	db, err := m.getOrOpenDB(resourcePath)
	if err != nil {
		return fmt.Errorf("failed to get DB: %w", err)
	}

	// Encode key
	encodedKey := encodeKey(key)

	// Delete from Pebble
	wo := &pebble.WriteOptions{}
	if err := db.Delete(encodedKey, wo); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to delete key: %w", err)
	}

	// Remove from expiry index
	m.removeFromExpiryIndex(resourcePath, key)

	return nil
}

// Exists checks if a key exists in the KV store
func (m *Manager) Exists(ctx context.Context, resourcePath, key string) (bool, error) {
	if key == "" {
		return false, InvalidKeyError{Key: key, Reason: "key cannot be empty"}
	}

	// Validate KV store exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return false, KVStoreNotFoundError{ResourcePath: resourcePath}
		}
		return false, fmt.Errorf("failed to validate KV store: %w", err)
	}

	// Get or open DB
	db, err := m.getOrOpenDB(resourcePath)
	if err != nil {
		return false, fmt.Errorf("failed to get DB: %w", err)
	}

	// Encode key
	encodedKey := encodeKey(key)

	// Check existence in Pebble
	_, closer, err := db.Get(encodedKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check key existence: %w", err)
	}
	closer.Close()

	return true, nil
}

// ListKeys lists all keys in a KV store, optionally filtered by prefix
func (m *Manager) ListKeys(ctx context.Context, resourcePath, prefix string) ([]string, error) {
	// Validate KV store exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, KVStoreNotFoundError{ResourcePath: resourcePath}
		}
		return nil, fmt.Errorf("failed to validate KV store: %w", err)
	}

	// Get or open DB
	db, err := m.getOrOpenDB(resourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get DB: %w", err)
	}

	// Build prefix key
	prefixKey := encodeKey(prefix)

	// Build upper bound for prefix
	var upperBoundBytes []byte
	if prefix != "" {
		upperBoundBytes = make([]byte, len(prefixKey))
		copy(upperBoundBytes, prefixKey)
		for i := len(upperBoundBytes) - 1; i >= 0; i-- {
			if upperBoundBytes[i] < 0xff {
				upperBoundBytes[i]++
				upperBoundBytes = upperBoundBytes[:i+1]
				break
			}
		}
	}

	var keys []string
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefixKey,
		UpperBound: upperBoundBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		encodedKey := iter.Key()

		// Decode key to get user key
		userKey := decodeKey(encodedKey)

		// Filter by prefix if specified
		if prefix != "" && !startsWith(userKey, prefix) {
			continue
		}

		keys = append(keys, userKey)
	}

	return keys, nil
}

// InitializeKVStore initializes a KV store by opening its database
func (m *Manager) InitializeKVStore(ctx context.Context, resourcePath string) error {
	// Validate KV store exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return KVStoreNotFoundError{ResourcePath: resourcePath}
		}
		return fmt.Errorf("failed to validate KV store: %w", err)
	}

	// Open DB (this will create it if it doesn't exist)
	_, err = m.getOrOpenDB(resourcePath)
	if err != nil {
		return fmt.Errorf("failed to initialize KV store: %w", err)
	}

	return nil
}

// addToExpiryIndex adds a key to the expiry index
func (m *Manager) addToExpiryIndex(expiresAt time.Time, resourcePath, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry := expiryEntry{resourcePath: resourcePath, key: key}
	m.expiryIndex[expiresAt] = append(m.expiryIndex[expiresAt], entry)
}

// removeFromExpiryIndex removes a key from the expiry index
func (m *Manager) removeFromExpiryIndex(resourcePath, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Iterate through expiry index to find and remove the entry
	for expiresAt, entries := range m.expiryIndex {
		for i, entry := range entries {
			if entry.resourcePath == resourcePath && entry.key == key {
				// Remove entry
				m.expiryIndex[expiresAt] = append(entries[:i], entries[i+1:]...)
				if len(m.expiryIndex[expiresAt]) == 0 {
					delete(m.expiryIndex, expiresAt)
				}
				return
			}
		}
	}
}

// ensureDirectory ensures a directory exists
func ensureDirectory(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.MkdirAll(path, 0755)
	}
	return nil
}

// startsWith checks if a string starts with a prefix
func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// hashResourcePath creates a simple hash from resource path
// Matches the hashing approach used in storage/dir.go
func hashResourcePath(path string) string {
	var hash uint32 = 2166136261
	for _, c := range path {
		hash ^= uint32(c)
		hash *= 16777619
	}
	hashStr := fmt.Sprintf("%x", hash)
	if len(hashStr) > 8 {
		hashStr = hashStr[:8]
	}
	return hashStr
}

// Recover recovers the state of the KV manager (rebuilds expiry index)
func (m *Manager) Recover(ctx context.Context) error {
	m.log.Info().Msg("Starting KV recovery")

	// List all KV resources
	kvs, err := m.metaStore.ListResources("", "", metastore.ResourceKV)
	if err != nil {
		return fmt.Errorf("failed to list KV stores: %w", err)
	}

	m.log.Info().Int("count", len(kvs)).Msg("Found KV stores to recover")

	count := 0
	for _, config := range kvs {
		if err := m.recoverKV(ctx, config.GetPath()); err != nil {
			m.log.Error().Err(err).Str("resource", config.GetPath()).Msg("Failed to recover KV store")
			// Continue recovering others
			continue
		}
		count++
	}

	m.log.Info().Int("recovered", count).Msg("KV recovery completed")
	return nil
}

// recoverKV recovers a single KV store
func (m *Manager) recoverKV(ctx context.Context, resourcePath string) error {
	// Open DB
	db, err := m.getOrOpenDB(resourcePath)
	if err != nil {
		return err
	}

	// Iterate all keys
	iter, err := db.NewIter(nil)
	if err != nil {
		return fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	keysCount := 0
	expiredCount := 0

	for iter.First(); iter.Valid(); iter.Next() {
		// We need to read the value to check expiration
		valueBytes, err := iter.ValueAndErr()
		if err != nil {
			continue
		}

		// Decode value
		val, err := decodeValue(valueBytes)
		if err != nil {
			m.log.Warn().Err(err).Str("resource", resourcePath).Msg("Failed to decode value during recovery")
			continue
		}

		// Check if it has expiry
		if val.ExpiresAt != nil {
			keysCount++
			userKey := decodeKey(iter.Key())

			// If already expired, we could delete it now, or let the reaper handle it
			// Adding to index means reaper will pick it up immediately
			m.addToExpiryIndex(*val.ExpiresAt, resourcePath, userKey)

			if time.Now().After(*val.ExpiresAt) {
				expiredCount++
			}
		}
	}

	if keysCount > 0 {
		m.log.Debug().
			Str("resource", resourcePath).
			Int("keys_with_ttl", keysCount).
			Int("expired", expiredCount).
			Msg("Recovered TTL keys")
	}

	return nil
}

// Scan scans the KV store for keys matching the prefix and optional filter
func (m *Manager) Scan(ctx context.Context, resourcePath, prefix, filterStr string, limit int) ([]ScanResult, error) {
	// Start tracing span
	_, span := StartScanSpan(ctx, resourcePath)
	defer span.End()

	// Parse filter if present
	var filterExpr filter.Expression
	if filterStr != "" {
		var err error
		filterExpr, err = filter.Parse(filterStr)
		if err != nil {
			return nil, fmt.Errorf("invalid filter expression: %w", err)
		}
	}

	// Validate limit
	if limit <= 0 {
		limit = 100 // Default to 100
	}
	if limit > 1000 {
		limit = 1000 // Hard cap
	}

	// Validate KV store exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, KVStoreNotFoundError{ResourcePath: resourcePath}
		}
		return nil, fmt.Errorf("failed to validate KV store: %w", err)
	}

	// Get or open DB
	db, err := m.getOrOpenDB(resourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get DB: %w", err)
	}

	// Build prefix key
	prefixKey := encodeKey(prefix)

	// Build upper bound for prefix if provided (optimization for lexicographical scan)
	var upperBoundBytes []byte
	if prefix != "" {
		upperBoundBytes = make([]byte, len(prefixKey))
		copy(upperBoundBytes, prefixKey)
		for i := len(upperBoundBytes) - 1; i >= 0; i-- {
			if upperBoundBytes[i] < 0xff {
				upperBoundBytes[i]++
				upperBoundBytes = upperBoundBytes[:i+1]
				break
			}
		}
	}

	results := make([]ScanResult, 0, limit)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefixKey,
		UpperBound: upperBoundBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if len(results) >= limit {
			break
		}

		encodedKey := iter.Key()
		userKey := decodeKey(encodedKey)

		// Filter by prefix (double check)
		if prefix != "" && !strings.HasPrefix(userKey, prefix) {
			continue
		}

		// Read value to filter
		valueBytes, err := iter.ValueAndErr()
		if err != nil {
			m.log.Warn().Err(err).Str("key", userKey).Msg("Failed to read value during scan")
			continue
		}

		val, err := decodeValue(valueBytes)
		if err != nil {
			m.log.Warn().Err(err).Str("key", userKey).Msg("Failed to decode value during scan")
			continue
		}

		// Check expiration
		if val.ExpiresAt != nil && time.Now().After(*val.ExpiresAt) {
			continue // Skip expired
		}

		// Apply filter
		if filterExpr != nil {
			// Try to unmarshal JSON value
			var jsonValue interface{}
			_ = json.Unmarshal(val.Payload, &jsonValue)

			matchCtx := filter.Context{
				"key":        userKey,
				"value":      string(val.Payload), // Raw string value
				"json":       jsonValue,           // Parsed JSON (if valid)
				"created_at": val.CreatedAt.Unix(),
			}

			if val.ExpiresAt != nil {
				matchCtx["expires_at"] = val.ExpiresAt.Unix()
			}

			result, err := filterExpr.Evaluate(matchCtx)
			if err != nil {
				// Log debug
				continue
			}
			if b, ok := result.(bool); !ok || !b {
				continue
			}
		}

		results = append(results, ScanResult{
			Key:   userKey,
			Value: val,
		})
	}

	return results, nil
}
