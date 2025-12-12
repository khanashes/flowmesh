package consumers

import (
	"fmt"
	"time"
)

// ConsumerOffset represents a committed offset for a consumer group
type ConsumerOffset struct {
	// Stream is the resource path of the stream
	Stream string `json:"stream"`
	// Group is the consumer group name
	Group string `json:"group"`
	// Partition is the partition ID (0 for MVP)
	Partition int32 `json:"partition"`
	// Offset is the committed offset
	Offset int64 `json:"offset"`
	// UpdatedAt is the last update timestamp
	UpdatedAt time.Time `json:"updated_at"`
}

// ConsumerGroupState represents the current state of a consumer group
type ConsumerGroupState struct {
	// Stream is the resource path of the stream
	Stream string `json:"stream"`
	// Group is the consumer group name
	Group string `json:"group"`
	// Partition is the partition ID
	Partition int32 `json:"partition"`
	// CommittedOffset is the last committed offset (-1 if no commits yet)
	CommittedOffset int64 `json:"committed_offset"`
	// LatestOffset is the latest available offset in the stream (-1 if no messages)
	LatestOffset int64 `json:"latest_offset"`
	// Lag is the calculated lag (latestOffset - committedOffset)
	Lag int64 `json:"lag"`
}

// offsetKey creates a unique key for an offset entry
func offsetKey(stream, group string, partition int32) string {
	return fmt.Sprintf("%s|%s|%d", stream, group, partition)
}
