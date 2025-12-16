package consumers

import (
	"strings"
)

const (
	// SandboxGroupPrefix is the prefix for sandbox consumer groups (used for replay)
	SandboxGroupPrefix = "_replay_"
)

// IsSandboxGroup checks if a consumer group name is a sandbox group
func IsSandboxGroup(groupName string) bool {
	return strings.HasPrefix(groupName, SandboxGroupPrefix)
}

// ValidateSandboxGroup validates that a consumer group name is a valid sandbox group
func ValidateSandboxGroup(groupName string) bool {
	return IsSandboxGroup(groupName)
}
