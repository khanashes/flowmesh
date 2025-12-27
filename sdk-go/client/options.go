package client

import (
	"time"
)

// Option is a functional option for client configuration
type Option func(*Client)

// WithAuthToken sets the authentication token
func WithAuthToken(token string) Option {
	return func(c *Client) {
		c.authToken = token
	}
}

// EnqueueOptions holds options for enqueueing a job
type EnqueueOptions struct {
	Delay   time.Duration
	Headers map[string]string
}

// EnqueueOption is a functional option for enqueue
type EnqueueOption func(*EnqueueOptions)

// WithDelay sets the delay before the job becomes visible
func WithDelay(delay time.Duration) EnqueueOption {
	return func(opts *EnqueueOptions) {
		opts.Delay = delay
	}
}

// WithHeaders sets headers for the job
func WithHeaders(headers map[string]string) EnqueueOption {
	return func(opts *EnqueueOptions) {
		opts.Headers = headers
	}
}

// ReserveOptions holds options for reserving/receiving jobs
type ReserveOptions struct {
	VisibilityTimeout time.Duration
	LongPollTimeout   time.Duration
}

// ReserveOption is a functional option for reserve/receive
type ReserveOption func(*ReserveOptions)

// WithVisibilityTimeout sets the visibility timeout
func WithVisibilityTimeout(timeout time.Duration) ReserveOption {
	return func(opts *ReserveOptions) {
		opts.VisibilityTimeout = timeout
	}
}

// WithLongPollTimeout sets the long poll timeout (0 = no wait)
func WithLongPollTimeout(timeout time.Duration) ReserveOption {
	return func(opts *ReserveOptions) {
		opts.LongPollTimeout = timeout
	}
}

// ReceiveOptions holds options for receiving multiple jobs
type ReceiveOptions struct {
	MaxJobs           int32
	VisibilityTimeout time.Duration
	LongPollTimeout   time.Duration
}

// ReceiveOption is a functional option for receive
type ReceiveOption func(*ReceiveOptions)

// WithMaxJobs sets the maximum number of jobs to receive (1-100)
func WithMaxJobs(maxJobs int32) ReceiveOption {
	return func(opts *ReceiveOptions) {
		if maxJobs < 1 {
			maxJobs = 1
		}
		if maxJobs > 100 {
			maxJobs = 100
		}
		opts.MaxJobs = maxJobs
	}
}

// WithReceiveVisibilityTimeout sets the visibility timeout for receive operations
func WithReceiveVisibilityTimeout(timeout time.Duration) ReceiveOption {
	return func(opts *ReceiveOptions) {
		opts.VisibilityTimeout = timeout
	}
}

// WithReceiveLongPollTimeout sets the long poll timeout for receive operations
func WithReceiveLongPollTimeout(timeout time.Duration) ReceiveOption {
	return func(opts *ReceiveOptions) {
		opts.LongPollTimeout = timeout
	}
}

// NACKOptions holds options for NACKing a job
type NACKOptions struct {
	Delay time.Duration // Optional delay before requeue (0 = use backoff)
}

// NACKOption is a functional option for NACK
type NACKOption func(*NACKOptions)

// WithNACKDelay sets the delay before requeue
func WithNACKDelay(delay time.Duration) NACKOption {
	return func(opts *NACKOptions) {
		opts.Delay = delay
	}
}

// KVSetOptions holds options for setting a KV value
type KVSetOptions struct {
	TTL time.Duration
}

// KVSetOption is a functional option for KV Set
type KVSetOption func(*KVSetOptions)

// WithTTL sets the TTL for a KV value
func WithTTL(ttl time.Duration) KVSetOption {
	return func(opts *KVSetOptions) {
		opts.TTL = ttl
	}
}

// SubscribeOptions holds options for subscribing to a stream
type SubscribeOptions struct {
	Partition   int32
	StartOffset int64 // -1 for committed offset, -2 for latest
}

// SubscribeOption is a functional option for subscribe
type SubscribeOption func(*SubscribeOptions)

// WithPartition sets the partition ID
func WithPartition(partition int32) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.Partition = partition
	}
}

// WithStartOffset sets the start offset (-1 for committed, -2 for latest)
func WithStartOffset(offset int64) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.StartOffset = offset
	}
}
