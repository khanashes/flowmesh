package client

import (
	"time"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
)

// Event represents an event to be written to a stream
type Event struct {
	Payload []byte
	Headers map[string]string
}

// ToProto converts Event to proto Event
func (e *Event) ToProto() *flowmeshpb.Event {
	return &flowmeshpb.Event{
		Payload: e.Payload,
		Headers: e.Headers,
	}
}

// Message represents a message read from a stream
type Message struct {
	ID            string
	ResourcePath  string
	Partition     int32
	Offset        int64
	Payload       []byte
	Headers       map[string]string
	CreatedAt     time.Time
	SchemaVersion int32
}

// FromProto creates a Message from proto Message
func MessageFromProto(m *flowmeshpb.Message) *Message {
	return &Message{
		ID:            m.Id,
		ResourcePath:  m.ResourcePath,
		Partition:     m.Partition,
		Offset:        m.Offset,
		Payload:       m.Payload,
		Headers:       m.Headers,
		CreatedAt:     time.Unix(0, m.CreatedAt),
		SchemaVersion: m.SchemaVersion,
	}
}

// Job represents a job in a queue
type Job struct {
	ID           string
	ResourcePath string
	Seq          int64
	Payload      []byte
	Headers      map[string]string
	CreatedAt    time.Time
	VisibleAt    time.Time
	ReserveUntil time.Time
	Attempts     int32
}

// FromProto creates a Job from proto Job
func JobFromProto(j *flowmeshpb.Job) *Job {
	return &Job{
		ID:           j.Id,
		ResourcePath: j.ResourcePath,
		Seq:          j.Seq,
		Payload:      j.Payload,
		Headers:      j.Headers,
		CreatedAt:    time.Unix(0, j.CreatedAt),
		VisibleAt:    time.Unix(0, j.VisibleAt),
		ReserveUntil: time.Unix(0, j.ReserveUntil),
		Attempts:     j.Attempts,
	}
}

// QueueStats represents statistics for a queue
type QueueStats struct {
	TotalJobs           int64
	PendingJobs         int64
	InFlightJobs        int64
	CompletedJobs       int64
	FailedJobs          int64
	OldestJobAgeSeconds int64
}

// FromProto creates QueueStats from proto QueueStats
func QueueStatsFromProto(s *flowmeshpb.QueueStats) *QueueStats {
	return &QueueStats{
		TotalJobs:           s.TotalJobs,
		PendingJobs:         s.PendingJobs,
		InFlightJobs:        s.InFlightJobs,
		CompletedJobs:       s.CompletedJobs,
		FailedJobs:          s.FailedJobs,
		OldestJobAgeSeconds: s.OldestJobAgeSeconds,
	}
}

// RetryPolicy represents a retry policy configuration
type RetryPolicy struct {
	MaxAttempts           int32
	InitialBackoffSeconds int64
	MaxBackoffSeconds     int64
	BackoffMultiplier     float64
	BackoffStrategy       string // "fixed", "linear", or "exponential"
}

// ToProto converts RetryPolicy to proto RetryPolicy
func (r *RetryPolicy) ToProto() *flowmeshpb.RetryPolicy {
	return &flowmeshpb.RetryPolicy{
		MaxAttempts:           r.MaxAttempts,
		InitialBackoffSeconds: r.InitialBackoffSeconds,
		MaxBackoffSeconds:     r.MaxBackoffSeconds,
		BackoffMultiplier:     r.BackoffMultiplier,
		BackoffStrategy:       r.BackoffStrategy,
	}
}

// FromProto creates RetryPolicy from proto RetryPolicy
func RetryPolicyFromProto(r *flowmeshpb.RetryPolicy) *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts:           r.MaxAttempts,
		InitialBackoffSeconds: r.InitialBackoffSeconds,
		MaxBackoffSeconds:     r.MaxBackoffSeconds,
		BackoffMultiplier:     r.BackoffMultiplier,
		BackoffStrategy:       r.BackoffStrategy,
	}
}

// ConsumerGroupState represents the state of a consumer group
type ConsumerGroupState struct {
	Stream          string
	Group           string
	Partition       int32
	CommittedOffset int64
	LatestOffset    int64
	Lag             int64
}

// FromProto creates ConsumerGroupState from proto ConsumerGroupState
func ConsumerGroupStateFromProto(s *flowmeshpb.ConsumerGroupState) *ConsumerGroupState {
	return &ConsumerGroupState{
		Stream:          s.Stream,
		Group:           s.Group,
		Partition:       s.Partition,
		CommittedOffset: s.CommittedOffset,
		LatestOffset:    s.LatestOffset,
		Lag:             s.Lag,
	}
}
