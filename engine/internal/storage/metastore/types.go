package metastore

import (
	"time"
)

// ResourceType represents the type of resource
type ResourceType string

const (
	// ResourceStream represents an event stream
	ResourceStream ResourceType = "stream"
	// ResourceQueue represents a job queue
	ResourceQueue ResourceType = "queue"
	// ResourceKV represents a key-value store
	ResourceKV ResourceType = "kv"
	// ResourceSchema represents a schema definition
	ResourceSchema ResourceType = "schema"
)

// RetentionConfig defines retention policy for resources
type RetentionConfig struct {
	// MaxAge is the maximum age of messages before deletion (0 = no limit)
	MaxAge time.Duration `json:"max_age,omitempty"`
	// MaxSize is the maximum size in bytes before deletion (0 = no limit)
	MaxSize int64 `json:"max_size,omitempty"`
	// MaxMessages is the maximum number of messages (0 = no limit)
	MaxMessages int64 `json:"max_messages,omitempty"`
}

// DLQConfig defines dead-letter queue configuration
type DLQConfig struct {
	// Enabled enables dead-letter queue
	Enabled bool `json:"enabled"`
	// MaxAttempts is the maximum number of delivery attempts before moving to DLQ
	MaxAttempts int32 `json:"max_attempts,omitempty"`
	// DLQPath is the resource path of the DLQ (optional, auto-generated if empty)
	DLQPath string `json:"dlq_path,omitempty"`
}

// RetryPolicyConfig defines retry behavior for failed jobs (for queues)
// This mirrors queues.RetryPolicy but is defined here to avoid circular dependencies
type RetryPolicyConfig struct {
	// MaxAttempts is the maximum number of delivery attempts (0 = unlimited)
	MaxAttempts int32 `json:"max_attempts"`
	// InitialBackoffSeconds is the initial delay before retry in seconds
	InitialBackoffSeconds int64 `json:"initial_backoff_seconds"`
	// MaxBackoffSeconds is the maximum delay between retries in seconds
	MaxBackoffSeconds int64 `json:"max_backoff_seconds"`
	// BackoffMultiplier is the multiplier for exponential backoff
	BackoffMultiplier float64 `json:"backoff_multiplier"`
	// BackoffStrategy is the strategy for calculating backoff (fixed, linear, exponential)
	BackoffStrategy string `json:"backoff_strategy"`
}

// SchemaRef references a schema for validation
type SchemaRef struct {
	// ID is the unique schema identifier
	ID string `json:"id"`
	// Type is the schema type (e.g., "jsonschema")
	Type string `json:"type"`
	// Version is the schema version
	Version int32 `json:"version"`
	// Definition is the raw schema definition (JSON)
	Definition []byte `json:"definition,omitempty"`
}

// ResourceConfig represents the configuration for a resource
type ResourceConfig struct {
	// Tenant is the tenant identifier
	Tenant string `json:"tenant"`
	// Namespace is the namespace within the tenant
	Namespace string `json:"namespace"`
	// Name is the resource name
	Name string `json:"name"`
	// Type is the resource type
	Type ResourceType `json:"type"`
	// Partitions is the number of partitions (for streams/queues)
	Partitions int `json:"partitions"`
	// Retention defines the retention policy
	Retention RetentionConfig `json:"retention,omitempty"`
	// DLQ defines dead-letter queue configuration (for queues)
	DLQ *DLQConfig `json:"dlq,omitempty"`
	// RetryPolicy defines retry behavior (for queues)
	RetryPolicy *RetryPolicyConfig `json:"retry_policy,omitempty"`
	// Schema references a schema for validation
	Schema *SchemaRef `json:"schema,omitempty"`
	// CreatedAt is when the resource was created
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt is when the resource was last updated
	UpdatedAt time.Time `json:"updated_at"`
}

// ResourcePath constructs the resource path from components
func ResourcePath(tenant, namespace, resourceType, name string) string {
	return tenant + "/" + namespace + "/" + resourceType + "/" + name
}

// GetPath returns the full resource path for a config
func (rc *ResourceConfig) GetPath() string {
	return ResourcePath(rc.Tenant, rc.Namespace, string(rc.Type), rc.Name)
}

// Validate validates the resource configuration
func (rc *ResourceConfig) Validate() error {
	if rc.Tenant == "" {
		return InvalidConfigError{Field: "tenant", Reason: "cannot be empty"}
	}
	if rc.Namespace == "" {
		return InvalidConfigError{Field: "namespace", Reason: "cannot be empty"}
	}
	if rc.Name == "" {
		return InvalidConfigError{Field: "name", Reason: "cannot be empty"}
	}
	if rc.Type != ResourceStream && rc.Type != ResourceQueue && rc.Type != ResourceKV && rc.Type != ResourceSchema {
		return InvalidConfigError{Field: "type", Reason: "must be stream, queue, kv, or schema"}
	}
	if rc.Partitions < 0 {
		return InvalidConfigError{Field: "partitions", Reason: "cannot be negative"}
	}
	if rc.Partitions == 0 {
		// Default to 1 partition for MVP
		rc.Partitions = 1
	}
	return nil
}
