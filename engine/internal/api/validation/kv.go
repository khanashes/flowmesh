package validation

import (
	"fmt"
	"time"
)

const (
	// MaxKeyLength is the maximum length of a key (1KB)
	MaxKeyLength = 1024
	// MaxValueSize is the maximum size of a value (1MB)
	MaxValueSize = 1024 * 1024
	// MaxTTLSeconds is the maximum TTL in seconds (1 year)
	MaxTTLSeconds = 365 * 24 * 60 * 60
)

// ValidateSetRequest validates a Set request
func ValidateSetRequest(resourcePath string, key string, value []byte, ttlSeconds int64) error {
	// Resource path validation is done at API layer via BuildResourcePath

	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	if len(key) > MaxKeyLength {
		return fmt.Errorf("key length (%d) exceeds maximum (%d)", len(key), MaxKeyLength)
	}

	if len(value) == 0 {
		return fmt.Errorf("value cannot be empty")
	}

	if len(value) > MaxValueSize {
		return fmt.Errorf("value size (%d bytes) exceeds maximum (%d bytes)", len(value), MaxValueSize)
	}

	if ttlSeconds < 0 {
		return fmt.Errorf("ttl_seconds cannot be negative")
	}

	if ttlSeconds > MaxTTLSeconds {
		return fmt.Errorf("ttl_seconds cannot exceed %d (1 year)", MaxTTLSeconds)
	}

	return nil
}

// ValidateGetRequest validates a Get request
func ValidateGetRequest(resourcePath string, key string) error {
	// Resource path validation is done at API layer via BuildResourcePath

	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	if len(key) > MaxKeyLength {
		return fmt.Errorf("key length (%d) exceeds maximum (%d)", len(key), MaxKeyLength)
	}

	return nil
}

// ValidateDeleteRequest validates a Delete request
func ValidateDeleteRequest(resourcePath string, key string) error {
	// Resource path validation is done at API layer via BuildResourcePath

	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	if len(key) > MaxKeyLength {
		return fmt.Errorf("key length (%d) exceeds maximum (%d)", len(key), MaxKeyLength)
	}

	return nil
}

// ValidateExistsRequest validates an Exists request
func ValidateExistsRequest(resourcePath string, key string) error {
	// Resource path validation is done at API layer via BuildResourcePath

	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	if len(key) > MaxKeyLength {
		return fmt.Errorf("key length (%d) exceeds maximum (%d)", len(key), MaxKeyLength)
	}

	return nil
}

// ValidateListKeysRequest validates a ListKeys request
func ValidateListKeysRequest(resourcePath string, prefix string) error {
	// Resource path validation is done at API layer via BuildResourcePath

	if len(prefix) > MaxKeyLength {
		return fmt.Errorf("prefix length (%d) exceeds maximum (%d)", len(prefix), MaxKeyLength)
	}

	return nil
}

// ValidateKey validates a key format
func ValidateKey(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	if len(key) > MaxKeyLength {
		return fmt.Errorf("key length (%d) exceeds maximum (%d)", len(key), MaxKeyLength)
	}

	return nil
}

// ValidateValue validates a value format
func ValidateValue(value []byte) error {
	if len(value) == 0 {
		return fmt.Errorf("value cannot be empty")
	}

	if len(value) > MaxValueSize {
		return fmt.Errorf("value size (%d bytes) exceeds maximum (%d bytes)", len(value), MaxValueSize)
	}

	return nil
}

// ValidateTTL validates a TTL duration
func ValidateTTL(ttl time.Duration) error {
	if ttl < 0 {
		return fmt.Errorf("TTL cannot be negative")
	}

	maxTTL := 365 * 24 * time.Hour
	if ttl > maxTTL {
		return fmt.Errorf("TTL cannot exceed %v (1 year)", maxTTL)
	}

	return nil
}

// ValidatePrefix validates a prefix format
func ValidatePrefix(prefix string) error {
	if len(prefix) > MaxKeyLength {
		return fmt.Errorf("prefix length (%d) exceeds maximum (%d)", len(prefix), MaxKeyLength)
	}

	return nil
}
