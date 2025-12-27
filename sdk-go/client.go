// Package flowmesh provides a Go client for FlowMesh
package flowmesh

import (
	"github.com/flowmesh/sdk-go/client"
)

// Re-export types for convenience
type (
	// Client is the main FlowMesh client
	Client = client.Client

	// Event represents an event to be written to a stream
	Event = client.Event

	// Message represents a message read from a stream
	Message = client.Message

	// Job represents a job in a queue
	Job = client.Job

	// QueueStats represents statistics for a queue
	QueueStats = client.QueueStats

	// RetryPolicy represents a retry policy configuration
	RetryPolicy = client.RetryPolicy

	// ConsumerGroupState represents the state of a consumer group
	ConsumerGroupState = client.ConsumerGroupState

	// Error represents an SDK error
	Error = client.Error
)

// Re-export functions
var (
	// NewClient creates a new FlowMesh client
	NewClient = client.NewClient

	// WithAuthToken sets the authentication token
	WithAuthToken = client.WithAuthToken

	// Enqueue options
	WithDelay   = client.WithDelay
	WithHeaders = client.WithHeaders

	// Reserve options
	WithVisibilityTimeout = client.WithVisibilityTimeout
	WithLongPollTimeout   = client.WithLongPollTimeout

	// Receive options
	WithMaxJobs                  = client.WithMaxJobs
	WithReceiveVisibilityTimeout = client.WithReceiveVisibilityTimeout
	WithReceiveLongPollTimeout   = client.WithReceiveLongPollTimeout

	// NACK options
	WithNACKDelay = client.WithNACKDelay

	// KV options
	WithTTL = client.WithTTL

	// Subscribe options
	WithPartition   = client.WithPartition
	WithStartOffset = client.WithStartOffset
)
