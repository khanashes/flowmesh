package validation

import (
	"fmt"
	"strings"
)

const (
	// MaxEventPayloadSize is the maximum size of an event payload in bytes (1MB)
	MaxEventPayloadSize = 1024 * 1024
	// MaxEventsPerRequest is the maximum number of events per WriteEvents request
	MaxEventsPerRequest = 1000
	// MaxMessagesPerRead is the maximum number of messages per ReadStream request
	MaxMessagesPerRead = 1000
	// MinOffset is the minimum valid offset value
	MinOffset int64 = 0
)

// ValidateOffset validates that an offset is non-negative
func ValidateOffset(offset int64) error {
	if offset < MinOffset && offset != -1 && offset != -2 {
		return fmt.Errorf("offset must be >= %d, -1 (committed), or -2 (latest)", MinOffset)
	}
	return nil
}

// ValidatePartition validates that a partition ID is non-negative
func ValidatePartition(partition int32) error {
	if partition < 0 {
		return fmt.Errorf("partition must be >= 0")
	}
	return nil
}

// ValidateEventPayload validates that an event payload is not empty and within size limits
func ValidateEventPayload(payload []byte) error {
	if len(payload) == 0 {
		return fmt.Errorf("event payload cannot be empty")
	}
	if len(payload) > MaxEventPayloadSize {
		return fmt.Errorf("event payload size (%d bytes) exceeds maximum (%d bytes)", len(payload), MaxEventPayloadSize)
	}
	return nil
}

// ValidateEventCount validates that the number of events is within limits
func ValidateEventCount(count int) error {
	if count <= 0 {
		return fmt.Errorf("at least one event is required")
	}
	if count > MaxEventsPerRequest {
		return fmt.Errorf("number of events (%d) exceeds maximum (%d)", count, MaxEventsPerRequest)
	}
	return nil
}

// ValidateMaxMessages validates that maxMessages is within limits
func ValidateMaxMessages(maxMessages int) error {
	if maxMessages <= 0 {
		return fmt.Errorf("max_messages must be > 0")
	}
	if maxMessages > MaxMessagesPerRead {
		return fmt.Errorf("max_messages (%d) exceeds maximum (%d)", maxMessages, MaxMessagesPerRead)
	}
	return nil
}

// ValidateConsumerGroupName validates that a consumer group name is valid
func ValidateConsumerGroupName(group string) error {
	if err := ValidateNonEmpty("consumer_group", group); err != nil {
		return err
	}

	// Consumer group names should be alphanumeric with dashes and underscores
	if len(group) > 255 {
		return fmt.Errorf("consumer_group name cannot exceed 255 characters")
	}

	// Check for invalid characters
	for _, char := range group {
		if !((char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '-' ||
			char == '_' ||
			char == '.') {
			return fmt.Errorf("consumer_group name contains invalid character: %c (allowed: alphanumeric, dash, underscore, dot)", char)
		}
	}

	return nil
}

// ValidateStreamResourcePath validates that a resource path is for a stream
func ValidateStreamResourcePath(tenant, namespace, name string) error {
	return ValidateResourcePathComponents(tenant, namespace, "stream", name)
}

// SanitizeConsumerGroupName sanitizes a consumer group name by removing invalid characters
func SanitizeConsumerGroupName(group string) string {
	var builder strings.Builder
	for _, char := range group {
		if (char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '-' ||
			char == '_' ||
			char == '.' {
			builder.WriteRune(char)
		}
	}
	result := builder.String()
	if len(result) > 255 {
		return result[:255]
	}
	return result
}
