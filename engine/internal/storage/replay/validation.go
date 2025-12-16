package replay

import (
	"fmt"
	"strings"
)

const (
	// SandboxGroupPrefix is the prefix for sandbox consumer groups
	SandboxGroupPrefix = "_replay_"
)

// ValidateSandboxGroup validates that a consumer group name is a valid sandbox group
func ValidateSandboxGroup(groupName string) error {
	if groupName == "" {
		return fmt.Errorf("sandbox consumer group name cannot be empty")
	}

	if !strings.HasPrefix(groupName, SandboxGroupPrefix) {
		return fmt.Errorf("sandbox consumer group name must start with %s", SandboxGroupPrefix)
	}

	return nil
}

// IsSandboxGroup checks if a consumer group name is a sandbox group
func IsSandboxGroup(groupName string) bool {
	return strings.HasPrefix(groupName, SandboxGroupPrefix)
}

// ValidateReplayRequest validates a replay start request
func ValidateReplayRequest(req StartRequest) error {
	if req.Stream == "" {
		return fmt.Errorf("stream is required")
	}

	if req.StartOffset == nil && req.StartTime == nil {
		return fmt.Errorf("either start_offset or start_time must be provided")
	}

	if req.StartOffset != nil && req.StartTime != nil {
		return fmt.Errorf("cannot specify both start_offset and start_time")
	}

	if req.StartOffset != nil && *req.StartOffset < 0 {
		return InvalidOffsetError{Offset: *req.StartOffset, Reason: "start offset cannot be negative"}
	}

	if req.EndOffset != nil && req.EndTime != nil {
		return fmt.Errorf("cannot specify both end_offset and end_time")
	}

	if req.StartOffset != nil && req.EndOffset != nil {
		if *req.EndOffset < *req.StartOffset {
			return InvalidOffsetError{Offset: *req.EndOffset, Reason: "end offset must be >= start offset"}
		}
	}

	if req.SandboxConsumerGroup == "" {
		return fmt.Errorf("sandbox_consumer_group is required")
	}

	if err := ValidateSandboxGroup(req.SandboxConsumerGroup); err != nil {
		return err
	}

	return nil
}
