package kv

import (
	"context"
	"time"
)

const (
	// DefaultTTLReaperInterval is the default interval for the TTL reaper
	DefaultTTLReaperInterval = 10 * time.Second
)

// runTTLReaper runs the background TTL reaper goroutine
func (m *Manager) runTTLReaper(ctx context.Context, stopCh <-chan struct{}) {
	ticker := time.NewTicker(DefaultTTLReaperInterval)
	defer ticker.Stop()

	m.log.Info().Dur("interval", DefaultTTLReaperInterval).Msg("TTL reaper started")

	for {
		select {
		case <-ctx.Done():
			m.log.Info().Msg("TTL reaper stopped due to context cancellation")
			return
		case <-stopCh:
			m.log.Info().Msg("TTL reaper stopped")
			return
		case <-ticker.C:
			m.reapExpiredKeys()
		}
	}
}

// reapExpiredKeys removes all expired keys from the KV store
func (m *Manager) reapExpiredKeys() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var expiredEntries []expiryEntry
	var expiredTimes []time.Time

	// Collect expired entries
	for expiresAt, entries := range m.expiryIndex {
		if expiresAt.Before(now) || expiresAt.Equal(now) {
			expiredEntries = append(expiredEntries, entries...)
			expiredTimes = append(expiredTimes, expiresAt)
		}
	}

	if len(expiredEntries) == 0 {
		return
	}

	// Remove expired entries from index
	for _, expiresAt := range expiredTimes {
		delete(m.expiryIndex, expiresAt)
	}

	m.mu.Unlock() // Release lock for Delete operations

	// Delete expired keys
	deletedCount := 0
	for _, entry := range expiredEntries {
		// Use background context since we're in a goroutine
		ctx := context.Background()
		if err := m.Delete(ctx, entry.resourcePath, entry.key); err != nil {
			// Key might have already been deleted or store might not exist
			// Log warning but continue
			m.log.Debug().
				Err(err).
				Str("resource_path", entry.resourcePath).
				Str("key", entry.key).
				Msg("Failed to delete expired key during reaping")
		} else {
			deletedCount++
		}
	}

	m.mu.Lock() // Re-acquire lock

	m.log.Debug().
		Int("deleted", deletedCount).
		Int("total_expired", len(expiredEntries)).
		Msg("TTL reaper completed")
}
